#!/bin/bash
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mainDir="${scriptDir}/.."
tcmalloc="env LD_PRELOAD=${HOME}/gperftools/.libs/libtcmalloc.so "

#/!\ This script will NOT work if you modify main.c

#
#Run YCSB A B C (D = A, F =B)
#4 workers per disk, 4 load injectors
#

rm -f /scratch*/tenvisdb/*.log  ##skip recovery
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
#use existing
#rm -f /scratch*/tenvisdb/*

cp ${mainDir}/main.c ${mainDir}/main.c.bak
#cat ${mainDir}/main.c | perl -pe 's://.nb_load_injectors = 4:.nb_load_injectors = 4:' | perl -pe 's:[^/].nb_load_injectors = 12: //.nb_load_injectors = 12:' | perl -pe 's:[^/]ycsb_e_uniform,: //ycsb_e_uniform,:' | perl -pe 's://ycsb_a_uniform,:ycsb_a_uniform,:' | perl -pe 's://ycsb_a_zipfian,:ycsb_a_zipfian,:' > ${mainDir}/main.c.tmp
mv ${mainDir}/main.c.tmp ${mainDir}/main.c
make -C ${mainDir} -j

iostat -xzd 1 > iostat.log &
vmstat 1 > vmstat.log &

echo "Run 1"
${tcmalloc} ${mainDir}/main 0 1 16 | tee log_ycsb_1

#echo "Run 2"
#${tcmalloc} ${mainDir}/main 0 1 32 | tee log_ycsb_2

mv ${mainDir}/main.c.bak ${mainDir}/main.c

#
#Show results
#
${scriptDir}/parse.pl log_ycsb_*
grep -v '[^ 0-9]' vmstat.log | awk '{if($15 < 90) {sum += 100 - $15; ++lines}} END {if (lines >0) printf "average CPU utilization is %s% \n", sum / lines}'

sudo kill -15 $(pidof iostat) 
sudo kill -15 $(pidof vmstat)

