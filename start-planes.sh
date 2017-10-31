#!/bin/bash

flink run -p 1 -c io.eventador.Planes ./target/flink-streaming-planes-1.0-SNAPSHOT.jar planes.properties
