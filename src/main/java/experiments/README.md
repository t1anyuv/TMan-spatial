# Experiments Commands

This directory uses `experiments.app.ExperimentApp` as the unified entry.

## Build

Package a runnable fat jar before using the plain `java` or `spark-submit` commands:

```bash
mvn -DskipTests package
```

Default jar path:

```text
target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Comparison

Run comparison experiments from a json config.

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp comparison src/main/resources/benchmark/comparison-config.sample.json
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar comparison src/main/resources/benchmark/comparison-config.sample.json
```

## Parameter

Run the LETI parameter experiment on TDrive or CD-Taxi.

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp parameter <query_base_dir> <data_file_path> <output_dir> [dataset_name]
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar parameter <query_base_dir> <data_file_path> <output_dir> [dataset_name]
```

`dataset_name` is optional. If omitted, the runner auto-detects from the input paths. Supported values: `Tdrive`, `CD-Taxi`.

## Similarity Query

Run unified top-k / similarity-query experiments from a json config.
Dataset config uses `queryWorkloadPath` for the similarity/top-k workload file.

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp similarity-query src/main/resources/benchmark/similarity-query-config.sample.json
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar similarity-query src/main/resources/benchmark/similarity-query-config.sample.json
```

## Ablation

Run the LETI ablation experiment.

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp ablation <query_base_dir> <data_file_path> <output_dir>
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar ablation <query_base_dir> <data_file_path> <output_dir>
```

## LMSFC Learn

Learn LMSFC theta from real trajectory data and generate a reordered LETI-based LMSFC order file.

Class roles:

```text
LMSFCLearningExperimentRunner -> experiment entry
LMSFCThetaLearner            -> theta learner
LMSFCOrderFileBuilder        -> order-file builder
```

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp lmsfc-learn <sample_data_path> <theta_output_path> <order_output_path> [base_order_path] [theta_bit_num] [candidate_count] [point_sample_limit] [seed]
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar lmsfc-learn <sample_data_path> <theta_output_path> <order_output_path> [base_order_path] [theta_bit_num] [candidate_count] [point_sample_limit] [seed]
```

## BMTree Learn

Learn a BMTree split config from sampled trajectories plus real query workloads, then generate the corresponding LETI-style order file.

Class roles:

```text
BMTreeLearningExperimentRunner -> experiment entry
BMTreeConfigLearner            -> config learner
BMTreeOrderFileBuilder         -> order-file builder
```

Java:

```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar experiments.app.ExperimentApp bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> <order_output_path> [base_order_path] [bit_length] [max_depth] [trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]
```

Spark-submit:

```bash
spark-submit --class experiments.app.ExperimentApp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> <order_output_path> [base_order_path] [bit_length] [max_depth] [trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]
```

## Notes

- `comparison`, `parameter`, and `ablation` may create tables and access HBase / Redis in the local environment.
- `similarity-query` may create tables and access HBase / Redis in the local environment.
- `lmsfc-learn` uses real trajectory points to learn theta, then reorders the effective nodes from the given LETI base order.
- `bmtree-learn` uses both sampled trajectories and real query workloads to learn the BMTree split policy, then reorders LETI effective nodes with the learned tree.
