#!/usr/bin/env bash
/home/ubuntu/apache-storm-0.10.2/bin/storm jar ~/apache-storm-0.10.2/examples/storm-starter/storm-starter-topologies-0.10.2.jar storm.starter.WordCountTopology ${1}
