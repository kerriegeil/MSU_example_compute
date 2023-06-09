#!/bin/bash

readarray -t sf < dummy_scalefactors.txt

echo ${sf[99]}
