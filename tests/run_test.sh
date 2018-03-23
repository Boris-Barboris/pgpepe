#!/bin/bash

psql -h localhost -U postgres -c 'drop database pgpepetestdb;'

set -eux
psql -h localhost -U postgres -c 'create database pgpepetestdb;'
dub -b debug
