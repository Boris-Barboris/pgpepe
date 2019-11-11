#!/bin/bash

psql -h 127.0.0.1 -U root -c 'drop database pgpepetestdb;'
set -eux
# dub test
psql -h 127.0.0.1 -U root -c 'create database pgpepetestdb;'
cd tests
dub -b unittest
