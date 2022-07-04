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
Graph of characters' relationships

| <img src="https://user-images.githubusercontent.com/30235642/177063489-16a6f282-4db0-42a7-8f66-8bb8202a906e.png" width="500" height="350"/> | <img src="https://user-images.githubusercontent.com/30235642/177063493-54e8593d-c7aa-4a4d-90f2-ea19d7c5dfbb.png" width="500" height="350"/> | 
| ----------- | ----------- |
