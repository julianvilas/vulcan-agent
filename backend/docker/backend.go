package docker

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/lestrrat-go/backoff"

	"github.com/adevinta/dockerutils"

	"github.com/adevinta/vulcan-agent/backend"
	"github.com/adevinta/vulcan-agent/log"
)

const (
	defaultDockerIfaceName = "docker0"
)

// RegistryConfig defines the configuration for the Docker registry.
type RegistryConfig struct {
	Server              string  `toml:"server"`
	User                string  `toml:"user"`
	Pass                string  `toml:"pass"`
	BackoffInterval     int     `toml:"backoff_interval"`
	BackoffMaxRetries   int     `toml:"backoff_max_retries"`
	BackoffJitterFactor float64 `toml:"backoff_jitter_factor"`
}

type Backend struct {
	config    RegistryConfig
	agentAddr string
	checkVars backend.CheckVars
	log       log.Logger
	cli       *dockerutils.Client
}

func NewBackend(log log.Logger, cfg RegistryConfig, APICfg backend.APIConfig, vars backend.CheckVars) (*Backend, error) {
	envCli, err := client.NewEnvClient()
	if err != nil {
		return &Backend{}, err
	}
	var addr string
	if APICfg.Host != "" {
		addr = APICfg.Host + APICfg.Port
	} else {
		addr, err = getAgentAddr(APICfg.Port, APICfg.IName)
	}

	cli := dockerutils.NewClient(envCli)
	if cfg.Server != "" {
		err = cli.Login(context.Background(), cfg.Server, cfg.User, cfg.Pass)
		if err != nil {
			return nil, err
		}
	}
	return &Backend{
		config:    cfg,
		agentAddr: addr,
		log:       log,
		checkVars: vars,
		cli:       cli,
	}, nil
}

// Run starts executing a check as a local container and returns a channel that
// will contain the result of the execution when it finishes.
func (b *Backend) Run(ctx context.Context, params backend.RunParams) (<-chan backend.RunResult, error) {
	err := b.pullWithBackoff(ctx, params.Image)
	if err != nil {
		return nil, err
	}
	var res = make(chan backend.RunResult)
	go b.run(ctx, params, res)
	return res, nil
}

func (b *Backend) run(ctx context.Context, params backend.RunParams, res chan<- backend.RunResult) {
	cfg := b.getRunConfig(params)
	contID, err := b.cli.Create(ctx, cfg, params.CheckID)
	if err != nil {
		res <- backend.RunResult{Error: err}
		return
	}
	defer func() {
		removeOpts := types.ContainerRemoveOptions{Force: true}
		removeErr := b.cli.ContainerRemove(context.Background(), contID, removeOpts)
		if removeErr != nil {
			b.log.Errorf("error removing container %s: %v", params.CheckID, err)
		}
	}()
	err = b.cli.ContainerStart(
		ctx, contID, cfg.ContainerStartOptions,
	)
	if err != nil {
		err := fmt.Errorf("error starting container for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	status, error := b.cli.ContainerWait(ctx, contID, container.WaitConditionNotRunning)
	select {
	case err = <-error:
	case <-status:
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.Canceled) {
		err := fmt.Errorf("error running container for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	logOpts := types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}
	r, err := b.cli.ContainerLogs(ctx, contID, logOpts)
	if err != nil {
		err := fmt.Errorf("error getting logs for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	defer r.Close()
	out, err := ioutil.ReadAll(r)
	if err != nil {
		err := fmt.Errorf("error reading logs for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	res <- backend.RunResult{Output: out}
}

func (b Backend) pullWithBackoff(ctx context.Context, image string) error {
	backoffPolicy := backoff.NewExponential(
		backoff.WithInterval(time.Duration(b.config.BackoffInterval*int(time.Second))),
		backoff.WithMaxRetries(b.config.BackoffMaxRetries),
		backoff.WithJitterFactor(b.config.BackoffJitterFactor),
	)
	bState, cancel := backoffPolicy.Start(context.Background())
	defer cancel()

	for backoff.Continue(bState) {
		err := b.cli.Pull(ctx, image)
		if err == nil {
			return nil
		}
		b.log.Errorf("Error pulling Docker image %s", image)
	}
	return errors.New("backoff retry exceeded pulling Docker image")
}

// getRunConfig will generate a docker.RunConfig for a given job.
// It will inject the check options and target as environment variables.
// It will return the generated docker.RunConfig.
func (b *Backend) getRunConfig(params backend.RunParams) dockerutils.RunConfig {
	checktypeName, checktypeVersion := getChecktypeInfo(params.Image)
	b.log.Debugf("fetching check variables from configuration %+v", params.RequiredVars)
	vars := dockerVars(params.RequiredVars, b.checkVars)
	return dockerutils.RunConfig{
		ContainerConfig: &container.Config{
			Hostname: params.CheckID,
			Image:    params.Image,
			Env: append([]string{
				fmt.Sprintf("%s=%s", backend.CheckIDVar, params.CheckID),
				fmt.Sprintf("%s=%s", backend.ChecktypeNameVar, checktypeName),
				fmt.Sprintf("%s=%s", backend.ChecktypeVersionVar, checktypeVersion),
				fmt.Sprintf("%s=%s", backend.CheckTargetVar, params.Target),
				fmt.Sprintf("%s=%s", backend.CheckAssetTypeVar, params.AssetType),
				fmt.Sprintf("%s=%s", backend.CheckOptionsVar, params.Options),
				fmt.Sprintf("%s=%s", backend.AgentAddressVar, b.agentAddr),
			},
				vars...,
			),
		},
		HostConfig:            &container.HostConfig{},
		NetConfig:             &network.NetworkingConfig{},
		ContainerStartOptions: types.ContainerStartOptions{},
	}
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

// dockerVars assigns the required environment variables in a format supported by Docker.
func dockerVars(requiredVars []string, envVars map[string]string) []string {
	var dockerVars []string

	for _, requiredVar := range requiredVars {
		dockerVars = append(dockerVars, fmt.Sprintf("%s=%s", requiredVar, envVars[requiredVar]))
	}

	return dockerVars
}

// getAgentAddr returns the current address of the agent API from the Docker network.
// It will also return any errors encountered while doing so.
func getAgentAddr(port, ifaceName string) (string, error) {
	connAddr, err := net.ResolveTCPAddr("tcp", port)
	if err != nil {
		return "", err
	}
	if ifaceName == "" {
		ifaceName = defaultDockerIfaceName
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

	return "", errors.New("failed to determine Docker agent IP address")
}
