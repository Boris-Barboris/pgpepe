#!/bin/bash

psql -h localhost -U postgres -c 'drop database pgpepetestdb;'
set -eux
# dub test
psql -h localhost -U postgres -c 'create database pgpepetestdb;'
cd tests
dub -b unittest
