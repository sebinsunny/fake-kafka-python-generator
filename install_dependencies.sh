#!/usr/bin/env bash

# Get repository
git clone https://github.com/raphlopez/python-fake-data-producer-for-apache-kafka.git
cd python-fake-data-producer-for-apache-kafka
git checkout raphlopez-load-testing
cd ..
pip install -r python-fake-data-producer-for-apache-kafka/requirements.txt
pip install aiven-client
