#!bin/bash

for file in $(ls hg38.chrom.*.sizes); do
  nohup python -u create_gene_chains.py $file >> log.txt 2>&1 &
done