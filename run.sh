#!/bin/sh

#TODO: Download the version according to the cluster version.
# This could be pulled into the docker image
wget https://storage.googleapis.com/kubernetes-release/release/v1.9.6/bin/linux/amd64/kubectl
chmod +x kubectl
mv kubectl /bin

# Apply env variables
cat config.toml | envsubst > run.toml

# Force logs to STDOUT
./vulcan-agent-kubernetes run.toml
