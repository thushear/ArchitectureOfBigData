#!/bin/bash

echo ""

echo -e "\nbuild docker hadoop image\n"
sudo docker build -t cdh/hadoop:6.0 .

echo ""
