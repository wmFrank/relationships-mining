# Relationships Mining in Jin's Novels

## Intro
This repo contains both Hadoop and Spark implement of mining and analynizing characters' relationships in Jin's novels.

## Datasets
15 well-known Jin's novels

## Workflow
- preprocess text(deduplicate characters in the same paragraphy)
- compute character co-occurrence
- group data for each characters and compute relationships weight
- apply [PageRank](https://en.wikipedia.org/wiki/PageRank) to get the importance of characters
- apply [Label Propagation](https://en.wikipedia.org/wiki/Label_propagation_algorithm) to cluster the charaters
- visualize the results with [gephi](https://gephi.org/)

## Results
