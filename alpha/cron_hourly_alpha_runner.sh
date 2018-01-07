#!/bin/bash
source ~/anaconda3/bin/activate
cd /home/ubuntu/lit_crypto/alpha
#for org in Y #input many facilites
#do # execute per loop? pass per loop?
./mt_alpha_runner.py --run 'Y' --runfocus_symbols_only 'N' --runcrawler 'N'
#done #for for loop