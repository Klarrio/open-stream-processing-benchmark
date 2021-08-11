# [OSPBench: Open Stream Processing Benchmark](https://github.com/Klarrio/open-stream-processing-benchmark/wiki)

This repository contains the code of the open stream processing benchmark.

All documentation can be found in our [wiki](https://github.com/Klarrio/open-stream-processing-benchmark/wiki).

It includes:
- [benchmark](./benchmark): benchmark pipeline implementations ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Benchmark)).
- [data-stream-generator](./data-stream-generator): data stream generator to generate input streams locally or on a DC/OS cluster ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Data-Stream-Generator)).
- [output-consumer](./output-consumer): consumes the output of the processing job and metrics-exporter from Kafka and stores it on S3 ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Output-Consumer)).
- [evaluator](./evaluator): computes performance metrics on the output of the output consumer ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Evaluation-Suite)).
- [result analysis](./result-analysis): Jupyter notebooks to visualize the results ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Results-and-analysis)).
- [deployment](./deployment): deployment scripts to run the benchmark on an DC/OS setup on AWS ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Architecture-and-deployment#deployment-on-aws-with-dcos)).
- [kafka-cluster-tools](./kafka-cluster-tools): Kafka scripts to start a cluster and read from a topic for local development ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Architecture-and-deployment#local-deployment-for-development)).
- [metrics-exporter](./metrics-exporter): exports metrics of JMX and cAdvisor and writes them to Kafka ([docs](https://github.com/Klarrio/open-stream-processing-benchmark/wiki/Metrics-Exporter)).

Currently the benchmark includes Apache Spark (Spark Streaming and Structured Streaming), Apache Flink and Kafka Streams.
## References, Publications and Talks
- [van Dongen, G., & Van den Poel, D. (2020). Evaluation of Stream Processing Frameworks. IEEE Transactions on Parallel and Distributed Systems, 31(8), 1845-1858.](https://ieeexplore.ieee.org/abstract/document/9025240)
The Supplemental Material of this paper can be found [here](https://s3.amazonaws.com/ieeecs.cdn.csdl.public/trans/td/2020/08/extras/ttd202008-09025240s1-supp1-2978480.pdf).

- [van Dongen, G., & Van den Poel, D. (2021). A Performance Analysis Fault Recovery in Stream Processing Frameworks. IEEE Access.](https://ieeexplore.ieee.org/document/9466838)

- [van Dongen, G., & Van den Poel, D. (2021). Influencing Factors in the Scalability of Distributed Stream Processing Jobs. IEEE Access.](https://ieeexplore.ieee.org/document/9507502)

- Earlier work-in-progress publication:
[van Dongen, G., Steurtewagen, B., & Van den Poel, D. (2018, July). Latency measurement of fine-grained operations in benchmarking distributed stream processing frameworks. In 2018 IEEE International Congress on Big Data (BigData Congress) (pp. 247-250). IEEE.](https://ieeexplore.ieee.org/document/8457759)
Talks related to this publication:

- Spark Summit Europe 2019: [Stream Processing: Choosing the Right Tool for the Job - Giselle van Dongen](https://www.youtube.com/watch?v=PiEQR9AXgl4&t=2s)

## Issues, questions or need help?

Are you having issues with anything related to the project? Do you wish to use this project or extend it? The fastest way to contact me is through:

LinkedIn: [giselle-van-dongen](https://www.linkedin.com/in/giselle-van-dongen/)

Email: giselle.vandongen@klarrio.com

