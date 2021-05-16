#!bin/bash

for file in $(ls formats/hg38.chrom.*.sizes); do
  nohup python -u create_gene_chains.py $file >> log200.txt 2>&1 &
done