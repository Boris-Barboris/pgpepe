#!/bin/bash

psql -h localhost -U postgres -c 'drop database pgpepetestdb;'
set -eux
dub test --skip-registry=all
psql -h localhost -U postgres -c 'create database pgpepetestdb;'
cd tests
dub -b unittest --skip-registry=all
