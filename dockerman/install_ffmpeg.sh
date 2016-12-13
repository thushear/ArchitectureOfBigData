#!/bin/bash
yum install  -y wget vim  autoconf automake gcc gcc-c++ git libtool make nasm pkgconfig zlib-devel yasm
cd /root
wget http://www.ffmpeg.org/releases/ffmpeg-3.2.tar.gz
tar -zxvf ffmpeg-3.2.tar.gz 
cd ffmpeg-3.2
./configure 
make
make install

