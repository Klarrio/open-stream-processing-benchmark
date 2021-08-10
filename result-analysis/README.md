# OSPBench Result Analysis - Jupyter Notebook
This article will lead you through the notebooks that are provided to generate visualizations and do further analysis.

This folder contains Jupyter notebooks with PySpark to do the analysis.  

You need Jupyter and Spark installed. Follow one of the many installation guides online.

Our packages have gotten slightly old. We used:

- Jupyter 4.3.0
- Python 3.6.6
- Spark 2.3.0

It is possible that some syntax changes are required for the latest versions of these tools.

Run Jupyter on the root of the result-analysis folder to have the paths correct in the notebooks.
For us this meant:

    cd result-analysis
    pyspark

Then open the notebooks at [localhost:8888](localhost:8888).

## Preparations
Initially, we used the structure for the input data which is used in the `data` folder. The latency, periodic burst and single burst notebooks make use of this.

Later on, we switched to a different structure: the one used in the folder `scalability-data`. The scalability and failure notebooks make use of this.

## The notebooks
There is one notebook per workload. The names of the notebook are similar to the names of the corresponding workloads.

The data which we used in our papers on OSPBench and the notebooks used to visualize them are both available here.
