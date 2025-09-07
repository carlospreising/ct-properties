#!/bin/bash

echo "Running apt-get update"
sudo apt-get update

echo "Installing Docker"
sudo apt-get -y install docker.io

echo "Docker without sudo setup..."
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart