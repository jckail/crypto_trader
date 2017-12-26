#!/bin/bash

for org in Y
do
./CtiRunner.py --org $org --skip-audit-build 'N' --skip-opa-build 'Y'
done
