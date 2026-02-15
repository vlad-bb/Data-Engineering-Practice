#!/bin/bash
apt-get update
apt-get install -y docker.io awscli
systemctl start docker
systemctl enable docker
usermod -aG docker ubuntu
