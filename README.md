# [OSPBench: Open Stream Processing Benchmark](https://github.com/Klarrio/ugent-phd-scalability-benchmark/wiki)

This repository contains the code of the open stream processing benchmark.

All documentation can be found in our [wiki](https://github.com/Klarrio/ugent-phd-scalability-benchmark/wiki).

It includes:
- benchmark pipeline implementations
- data stream generator to generate input streams locally or on a DC/OS cluster
- Kafka scripts to start a cluster and read from a topic for local development

Currently the benchmark includes Apache Spark (Spark Streaming and Structured Streaming), Apache Flink and Kafka Streams.
 structured-streaming-benchmark or kafka-benchmark

## References, Publications and Talks
- [van Dongen, G., & Van den Poel, D. (2020). Evaluation of Stream Processing Frameworks. IEEE Transactions on Parallel and Distributed Systems, 31(8), 1845-1858.](https://ieeexplore.ieee.org/abstract/document/9025240)
The Supplemental Material of this paper can be found [here](https://s3.amazonaws.com/ieeecs.cdn.csdl.public/trans/td/2020/08/extras/ttd202008-09025240s1-supp1-2978480.pdf).

- Earlier work-in-progress publication:
[van Dongen, G., Steurtewagen, B., & Van den Poel, D. (2018, July). Latency measurement of fine-grained operations in benchmarking distributed stream processing frameworks. In 2018 IEEE International Congress on Big Data (BigData Congress) (pp. 247-250). IEEE.](https://ieeexplore.ieee.org/document/8457759)

Talks related to this publication: 

- Spark Summit Europe 2019: [Stream Processing: Choosing the Right Tool for the Job - Giselle van Dongen](https://www.youtube.com/watch?v=PiEQR9AXgl4&t=2s)
