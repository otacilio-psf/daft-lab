#!/bin/bash

mkdir -p ./data/raw

files=("title.basics.tsv.gz" "title.ratings.tsv.gz")

for entry in "${files[@]}"
do
    curl -o "./data/raw/${entry}" "https://datasets.imdbws.com/${entry}" && gzip -df "./data/raw/${entry}"
done
