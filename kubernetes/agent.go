package kubernetes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"sync"

	agent "github.com/adevinta/vulcan-agent"
	"github.com/adevinta/vulcan-agent/check"
	"github.com/adevinta/vulcan-agent/config"
	"github.com/sirupsen/logrus"
)

const (
	defaultPublicIfaceName = "eth0"
)

// CheckConfig stores the configuration required to run a check.
type CheckConfig struct {
	CheckAbortTimeout int                          // Time to wait for a check container to stop gracefully.
	CheckLogLevel     string                       // Log level for the check default logger.
	CheckVars         map[string]map[string]string // Environment variables to inject to checks.
	AgentAPIAddr      string                       // Address exposed by the agent API.
}

// Agent implements the agent.Agent interface.
// It holds the necessary information to do so for the Docker runtime environment.
type Agent struct {
	id      string
	addr    string
	status  string
	ctx     context.Context
	log     *logrus.Entry
	cancel  context.CancelFunc
	storage check.Storage
	config  config.Config
	mutex   sync.RWMutex
}

// NewAgent creates a new Agent.
// It returns a Agent and any errors encountered while creating it.
func NewAgent(ctx context.Context, cancel context.CancelFunc, id string, storage check.Storage, l *logrus.Entry, cfg config.Config) (agent.Agent, error) {
	addr, err := getAgentAddr(cfg.API.Port, cfg.API.IName)
	if err != nil {
		return &Agent{}, err
	}

	// Overwrite Kubectl configuration only if present in the configuration file.
	if cfg.Runtime.Kubernetes != (config.KubernetesConfig{}) {
		err = setKubectlConfig(cfg.Runtime.Kubernetes, l)
		if err != nil {
			return &Agent{}, err
		}
	}

	return &Agent{
		id:      id,
		addr:    addr,
		status:  agent.StatusNew,
		ctx:     ctx,
		log:     l,
		cancel:  cancel,
		storage: storage,
		config:  cfg,
		mutex:   sync.RWMutex{},
	}, nil
}

// ID returns the ID assigned when creating the Agent.
func (a *Agent) ID() string {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.id
}

// Status returns the current Status of the Agent.
func (a *Agent) Status() string {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.status
}

// SetStatus sets the current Status of the Agent.
func (a *Agent) SetStatus(status string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.status = status
}

// Run runs the job in the Kubernetes cluster.
// It will store the pod name in the job.Meta field.
// It will update the job stored in the jobs map.
// It will return any errors encountered.
func (a *Agent) Run(checkID string) error {
	var err error

	job, err := a.storage.Get(checkID)
	if err != nil {
		return err
	}

	envVars := a.getEnvVars(job)

	return runCmd(
		append(
			[]string{
				"kubectl", "run",
				// Pod name
				job.CheckID,
				// Never restart
				"--restart", "Never",
				// Kubernetes label
				"-l", "run=vulcan-check",
				// Docker image
				"--image", job.Image,
			},
			// Environment variables
			envVars...,
		),
		nil,
		a.log,
	)
}

// Kill will forcefully remove a container and return any error encountered.
func (a *Agent) Kill(checkID string) error {
	job, err := a.storage.Get(checkID)
	if err != nil {
		return err
	}

	return runCmd(
		[]string{"kubectl", "delete", "pod", "--grace-period", "0", job.CheckID},
		nil, a.log,
	)
}

// AbortChecks gets the all the checks belonging to a scan that are currently running
// and call abort in each of them.
func (a *Agent) AbortChecks(scanID string) error {
	jobs, err := a.storage.GetAllByStatus(check.StatusRunning)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.ScanID == scanID {
			err := a.Abort(j.CheckID)
			if err != nil {
				// TODO: Check if the error is because the pod is already
				// finished.
				return err
			}
		}
	}
	return nil
}

// Abort will send the SIGTERM signal to a check and wait for it to stop.
// It will return any error encountered while doing so.
func (a *Agent) Abort(checkID string) error {
	job, err := a.storage.Get(checkID)
	if err != nil {
		return err
	}

	timeout := a.config.Check.AbortTimeout

	// This command will send a SIGTERM signal to the entrypoint.
	// It will wait for the amount of seconds configured and then
	// forcefully delete the pod.
	return runCmd(
		[]string{
			"kubectl", "delete", "pod", "--grace-period",
			string(timeout), job.CheckID,
		},
		nil, a.log,
	)
}

// Raw returns the raw output of the check and any errors encountered.
// It will use the Docker API to retrieve the container logs.
func (a *Agent) Raw(checkID string) ([]byte, error) {
	job, err := a.storage.Get(checkID)
	if err != nil {
		return []byte{}, err
	}

	var out bytes.Buffer
	err = runCmd([]string{"kubectl", "logs", job.CheckID}, &out, a.log)

	return out.Bytes(), err
}

// getEnvVars will return the environment variable flags for a given job.
// It will inject the check options and target as environment variables.
// It will return the generated slice of flags.
func (a *Agent) getEnvVars(job check.Job) []string {
	checktypeName, checktypeVersion := getChecktypeInfo(job.Image)
	logLevel := a.config.Check.LogLevel

	vars := kubectlVars(job.RequiredVars, a.config.Check.Vars)

	return append(
		[]string{
			"--env", fmt.Sprintf("%s=%s", agent.CheckIDVar, job.CheckID),
			"--env", fmt.Sprintf("%s=%s", agent.ChecktypeNameVar, checktypeName),
			"--env", fmt.Sprintf("%s=%s", agent.ChecktypeVersionVar, checktypeVersion),
			"--env", fmt.Sprintf("%s=%s", agent.CheckTargetVar, job.Target),
			"--env", fmt.Sprintf("%s=%s", agent.CheckOptionsVar, job.Options),
			"--env", fmt.Sprintf("%s=%s", agent.CheckLogLevelVar, logLevel),
			"--env", fmt.Sprintf("%s=%s", agent.AgentAddressVar, a.addr),
		},
		vars...,
	)
}

// getAgentAddr returns the current address of the agent API from the Internet.
// It will also return any errors encountered while doing so.
func getAgentAddr(port, ifaceName string) (string, error) {
	connAddr, err := net.ResolveTCPAddr("tcp", port)
	if err != nil {
		return "", err
	}
	if ifaceName == "" {
		ifaceName = defaultPublicIfaceName
	}
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return "", err
		}

		// Check if it is IPv4.
		if ip.To4() != nil {
			connAddr.IP = ip
			return connAddr.String(), nil
		}
	}

	return "", errors.New("failed to determine agent public IP address")
}

// getChecktypeInfo extracts checktype data from a Docker image URI.
func getChecktypeInfo(imageURI string) (checktypeName string, checktypeVersion string) {
	// https://github.com/docker/distribution/blob/master/reference/reference.go#L1-L24
	re := regexp.MustCompile(`(?P<checktype_name>[a-z0-9]+(?:[-_.][a-z0-9]+)*):(?P<checktype_version>[\w][\w.-]{0,127})`)

	matches := re.FindStringSubmatch(imageURI)

	checktypeName = matches[1]
	checktypeVersion = matches[2]

	return
}

// setKubectlConfig sets the configuration for Kubectl.
func setKubectlConfig(config config.KubernetesConfig, log *logrus.Entry) error {
	err := runCmd(
		[]string{
			"kubectl", "config", "set-cluster",
			config.Cluster.Name, "--server", config.Cluster.Server,
		},
		nil, log,
	)
	if err != nil {
		return err
	}

	err = runCmd(
		[]string{
			"kubectl", "config", "set-context",
			config.Context.Name, "--cluster", config.Context.Cluster,
			"--namespace", config.Context.Namespace, "--user", config.Context.User,
		},
		nil, log,
	)
	if err != nil {
		return err
	}

	err = runCmd(
		[]string{
			"kubectl", "config", "set-credentials",
			config.Credentials.Name, "--token", config.Credentials.Token,
		},
		nil, log,
	)
	if err != nil {
		return err
	}

	return runCmd(
		[]string{"kubectl", "config", "set", "current-context", config.Context.Name},
		nil, log,
	)
}

// kubectlVars assigns the required environment variables in a format supported by Kubectl.
func kubectlVars(requiredVars []string, envVars map[string]string) []string {
	var kubectlVars []string

	for _, requiredVar := range requiredVars {
		kubectlVars = append(kubectlVars, "--env", fmt.Sprintf("%s=%s", requiredVar, envVars[requiredVar]))
	}

	return kubectlVars
}

// runCmd runs a system command, outputs to out and logs it in debug level.
func runCmd(cmdLine []string, out *bytes.Buffer, log *logrus.Entry) error {
	cmd := exec.Command(cmdLine[0], cmdLine[1:]...)
	if out != nil {
		cmd.Stdout = out
	}
	cmdLine = censorCmdLine(cmdLine)
	log.WithFields(logrus.Fields{"cmd": cmdLine}).Debug("running command")
	return cmd.Run()
}

// censorCmdLine removes potentially sensitive data from a command in order to log it.
func censorCmdLine(cmdLine []string) []string {
	envVar := regexp.MustCompile(`(.*?)=.*`)
	for i, v := range cmdLine {
		if v == "--token" {
			cmdLine[i+1] = "[CENSORED]"
		}
		if v == "--env" {
			if isRequiredVar(cmdLine[i+1]) {
				cmdLine[i+1] = envVar.ReplaceAllString(cmdLine[i+1], "$1=[CENSORED]")
			}
		}
	}

	return cmdLine
}

// isRequiredVar returns whether or not an environment variable is one required by the check.
func isRequiredVar(envVar string) bool {
	for _, agentVar := range []string{
		agent.CheckIDVar,
		agent.ChecktypeNameVar,
		agent.ChecktypeVersionVar,
		agent.CheckTargetVar,
		agent.CheckOptionsVar,
		agent.CheckLogLevelVar,
		agent.AgentAddressVar,
	} {
		if envVar == agentVar {
			return false
		}
	}

	return true
}
