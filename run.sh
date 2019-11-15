#!/bin/sh

if [ -f "$1" ]; then
  # Force logs to STDOUT
  cat $1 | sed 's/log_file *=.*/LogFile = ""/g' > config.toml
else
  echo "ERROR: Expected config file"
  echo "Usage: $0 config.toml"
fi

#TODO: Download the version according to the cluster version
wget https://storage.googleapis.com/kubernetes-release/release/v1.9.6/bin/linux/amd64/kubectl
chmod +x kubectl
mv kubectl /bin

# Force logs to STDOUT
./vulcan-agent-kubernetes config.toml
