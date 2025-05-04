# Correlation Clustering
This project implements a scalable **Combinatorial Correlation Clustering** algorithm on Apache Spark's GraphX framework, based on the 2 – 2⁄13 approximation from Cohen-Addad et al. (2024). The implementation is efficient for distributed environments, avoids expensive union-find steps, and performs well on large signed graphs.

---

## Problem Overview

Given a signed undirected graph $G = (V, E^+ \cup E^-)$, where edges are labeled as either "positive" or "negative," the goal of **correlation clustering** is to partition the vertex set \( V \) into clusters so that:

- Positive edges lie *within* clusters, and
- Negative edges lie *between* clusters.

The objective is to minimize the **disagreement cost**

This problem is NP-hard in general. The implemented algorithm offers a scalable and provably approximate solution.

---

## Algorithmic Strategy

The algorithm builds on the following two principles from Cohen-Addad et al.'s combinatorial approach:

### 1. α-Bucketed CC-Pivot

- Each vertex is assigned a hash-based random **rank** in [0,1).
- Vertices are grouped into **buckets** of width α (with default $\alpha = 2/13$, forming $ L = \lceil 1/\alpha \rceil = 7 $ total buckets.
- Within each bucket, singleton vertices evaluate the **net gain** of joining each neighboring cluster:
  $\text{gain}(S \to C) = |E^+(S,C)| - |E^-(S,C)|$
  and move greedily to the best available cluster if the gain is non-negative.

### 2. Boosting via Permutations

- The entire procedure is repeated over `T` independent random permutations (default: 5 trials).
- The clustering result with the lowest disagreement cost is retained.

This combination ensures high-quality clustering with bounded approximation guarantees and allows efficient parallelization in Spark.

---

## Approximation Guarantee

Cohen-Addad et al. (2024) prove that the described algorithm achieves an approximation ratio of:


$\text{Approximation Factor} = 2 - \frac{2}{13} \approx 1.846$

The approximation is achieved deterministically for each permutation. The boosting via multiple trials improves the probability of obtaining a near-optimal solution exponentially.

---

## Spark Implementation Details

The system is implemented using Spark's GraphX library. It supports graphs with millions or billions of edges by leveraging Spark's RDD abstraction and efficient partitioning strategy (`EdgePartition2D`).

### Breakdown by Stage

| Stage                  | Spark API Used                                   | Shuffle Stages |
|------------------------|--------------------------------------------------|----------------|
| Preprocessing          | `partitionBy`, `mapEdges`                        | 1              |
| Rank Initialization    | `mapVertices`                                    | 0              |
| Bucket Processing Loop | `filter`, `map`, `reduceByKey`, broadcast        | 4 per bucket   |
| Cost Evaluation        | `triplets.map`, `sum`                            | 1              |
| Best Result Selection  | `outerJoinVertices`                              | 1              |

- Each of the `T` trials runs independently and can be parallelized.
- Graphs are cached in memory for efficiency.
- All transformations are functional and distributed.

---

## Result
Experiments were conducted on a Google Cloud Dataproc cluster with the following configuration:

- **1 master node**: `n4-standard-2` (2 vCPU, 1 core, 8 GB memory)
- **2 worker nodes**: `n4-standard-2` (4 vCPU, 2 cores, 16 GB memory each)

### Runtime and Disagreement Metrics

| Dataset                      | Size       | Runtime        | Number of Disagreements |
|------------------------------|------------|----------------|--------------------------|
| `log_normal_100.csv`         | 18 KB      | 35 sec         | 2,259                    |
| `musae_ENGB_edges.csv`       | 377 KB     | 42 sec         | 713,379                  |
| `soc-pokec-relationships.csv`| 331.4 MB   | 7 min 57 sec   | 29,280,710               |
| `soc-LiveJournal1.csv`       | 670.6 MB   | 14 min 46 sec  | 54,195,818               |
| `twitter_original_edges`     | 990.8 MB   | 19 min 51 sec  | 63,801,170,558           |
| `com-orkut.ungraph`          | 1.89 GB    | 27 min 11 sec  | 403,271,165              |
