#!/bin/bash

SOURCE="http://fileshare.csb.univie.ac.at/vog"
TARGET=${1:-data}
VERSION=${2:-latest}

# create target directory

mkdir -p ${TARGET}

# clean up left-overs

cd ${TARGET}

# fetch all files from source

wget --no-verbose \
     --recursive --no-parent --no-host-directories --no-directories \
     --timestamping --accept 'vog*' \
     ${SOURCE}/${VERSION}/

# unzip FASTA files, but keep the original so that timestamping works

rm *.fa
gunzip -k *.fa.gz

# untar archives (not needed so far)

#for f in faa hmm raw_algs; do
#	mkdir ${f}
#	tar -x -z -C $f -f vog.$f.tar.gz
#	rm vog.$f.tar.gz
#done
