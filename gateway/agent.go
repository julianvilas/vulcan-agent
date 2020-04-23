package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	agent "github.com/adevinta/vulcan-agent"
	"github.com/adevinta/vulcan-agent/check"
	"github.com/adevinta/vulcan-agent/config"
	"github.com/sirupsen/logrus"
)

const (
	defaultPublicIfaceName = "eth0"
	censoredValue          = "[CENSORED]"
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

type GatewayRequest struct {
	CallID string `json:"CallID"` // X-Call-Id
}

// NewAgent creates a new Agent.
// It returns a Agent and any errors encountered while creating it.
func NewAgent(ctx context.Context, cancel context.CancelFunc, id string, storage check.Storage, l *logrus.Entry, cfg config.Config) (agent.Agent, error) {
	addr, err := getAgentAddr(cfg.API.Port, cfg.API.IName)
	if err != nil {
		return &Agent{}, err
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

func buildBody(vars map[string]string) string {
	var attrs []string
	for k, v := range vars {
		attrs = append(attrs, fmt.Sprintf("\"%s\":\"%s\"", k, v))
	}
	return fmt.Sprintf("{ %s }", strings.Join(attrs, ","))
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

	checktypeName, _ := getChecktypeInfo(job.Image)
	url := fmt.Sprintf("%s/%s", a.config.Runtime.Gateway.Endpoint, checktypeName)
	bodyStr := buildBody(a.getVars(job, false))

	a.log.WithFields(logrus.Fields{"check_id": checkID, "url": url, "body": buildBody(a.getVars(job, true))}).Debug("faas-begin")
	a.log.WithFields(logrus.Fields{"check_id": checkID, "url": url, "body": buildBody(a.getVars(job, true))}).Debug("faas-begin")

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(bodyStr)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Callback-Url", fmt.Sprintf("http://%s/logs/%s", a.addr, checkID))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)

	meta := GatewayRequest{CallID: resp.Header.Get("X-Call-Id")}

	a.log.WithFields(logrus.Fields{"check_id": checkID, "X-Call-Id": meta.CallID, "status": resp.StatusCode}).Debug("faas-end")

	if err := a.storage.SetMeta(job.CheckID, meta); err != nil {
		return err
	}

	return err
}

// Kill will forcefully remove a container and return any error encountered.
func (a *Agent) Kill(checkID string) error {
	//	job, err := a.storage.Get(checkID)
	//	if err != nil {
	//		return err
	//	}

	// Unable to Kill the check
	return nil
}

// AbortChecks gets the all the checks belonging to a scan that are currently running
// and call abort in each of them.
func (a *Agent) AbortChecks(scanID string) error {
	// Unable to abort the checks
	return nil
}

// Abort will send the SIGTERM signal to a check and wait for it to stop.
// It will return any error encountered while doing so.
func (a *Agent) Abort(checkID string) error {
	// Unable to abort the checks
	return nil
}

// Raw returns the raw output of the check and any errors encountered.
// It will use the Docker API to retrieve the container logs.
func (a *Agent) Raw(checkID string) ([]byte, error) {
	return []byte{}, nil
}

// getEnvVars will return the environment variable flags for a given job.
// It will inject the check options and target as environment variables.
// It will return the generated slice of flags.
func (a *Agent) getVars(job check.Job, censored bool) map[string]string {
	checktypeName, checktypeVersion := getChecktypeInfo(job.Image)
	logLevel := a.config.Check.LogLevel

	vars := make(map[string]string)
	for _, rv := range job.RequiredVars {
		if censored {
			vars[rv] = censoredValue
		} else {
			vars[rv] = a.config.Check.Vars[rv]
		}
	}
	vars[agent.CheckIDVar] = job.CheckID
	vars[agent.ChecktypeNameVar] = checktypeName
	vars[agent.ChecktypeVersionVar] = checktypeVersion
	vars[agent.CheckTargetVar] = job.Target
	vars[agent.CheckOptionsVar] = job.Options
	vars[agent.CheckLogLevelVar] = logLevel
	vars[agent.AgentAddressVar] = a.addr

	return vars
}

// getAgentAddr returns the current address of the agent API from the Internet.
// It will also return any errors encountered while doing<t so.
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
