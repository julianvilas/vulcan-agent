package docker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/adevinta/dockerutils"
	"github.com/docker/docker/client"
	"github.com/google/go-cmp/cmp"

	"github.com/adevinta/vulcan-agent/backend"
	"github.com/adevinta/vulcan-agent/log"
)

func TestIntegrationBackend_Run(t *testing.T) {
	type args struct {
		ctx    context.Context
		params backend.RunParams
	}
	tests := []struct {
		name       string
		setup      func() *Backend
		tearDown   func(b *Backend)
		args       args
		want       backend.RunResult
		wantRunErr error
		wantErr    bool
	}{
		{
			name: "ExecutesADockerContainer",
			setup: func() *Backend {
				envCli, err := client.NewEnvClient()
				if err != nil {
					panic(err)
				}
				cli := dockerutils.NewClient(envCli)
				b := &Backend{
					agentAddr: "an addr",
					log:       &log.NullLog{},
					cli:       cli,
				}
				err = buildDockerImage("testdata/Dockerfile", "vulcan-check")
				if err != nil {
					panic(err)
				}
				return b
			},
			tearDown: func(b *Backend) {
				removeContainer("CheckID")
			},
			args: args{
				context.Background(),
				backend.RunParams{
					CheckID: "CheckID",
					Image:   "vulcan-check:latest",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.setup()
			defer func() {
				if tt.tearDown != nil {
					tt.tearDown(b)
				}
			}()
			gotChan, err := b.Run(tt.args.ctx, tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("Backend.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := <-gotChan
			gotErr := got.Error
			if gotErr != tt.wantRunErr {
				t.Errorf("gotErr != wantErr, gotErr: %v, wantErr %v", gotErr, tt.wantErr)
			}
			diff := cmp.Diff(string(got.Output), string(tt.want.Output))
			if diff != "" {
				t.Errorf("got!=want, diff %s", diff)
			}

		})
	}
}

func TestIntegrationBackend_RunContainerIsKilled(t *testing.T) {
	envCli, err := client.NewEnvClient()
	if err != nil {
		panic(err)
	}
	cli := dockerutils.NewClient(envCli)
	b := &Backend{
		agentAddr: "an addr",
		log:       &log.NullLog{},
		cli:       cli,
	}
	err = buildDockerImage("testdata/Dockerfile", "vulcan-check")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	params := backend.RunParams{
		CheckID: "CheckID",
		Image:   "vulcan-check:latest",
	}
	gotChan, err := b.Run(ctx, params)
	if err != nil {
		t.Error(err)
		return
	}
	defer removeContainer("CheckID")
	//cancel()
	got := <-gotChan
	gotErr := got.Error
	if gotErr != nil {
		t.Errorf("%+v", gotErr)
		return
	}
}

func buildDockerImage(dockerFile string, tag string) (err error) {
	path, err := filepath.Abs(dockerFile)
	if err != nil {
		return nil
	}
	dir := filepath.Dir(path)
	args := []string{"build", "-t", tag, "-f", path, dir}
	cmd := exec.Command("docker", args...)
	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func removeContainer(name string) (err error) {
	args := []string{"rm", name}
	cmd := exec.Command("docker", args...)
	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
