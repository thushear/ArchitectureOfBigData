#!/bin/bash

echo $1
docker build -t $1 .
echo "\n build docker end \n"