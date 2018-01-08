#!/bin/bash
source ~/anaconda3/bin/activate
#for org in Y #input many facilites
#do # execute per loop? pass per loop?
./mt_alpha_runner.py --run 'Y' --runfocus_symbols_only 'Y' --runcrawler 'N' --runcrawler 'N' --minute_run_param 'N'
#done #for for loop
