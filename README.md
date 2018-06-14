<div align="center"><img src="https://www.hellofresh.com/images/hellofresh-logo.svg" width="400"/></div>

# recipes-etl: ETL on recipes

*recipes-etl* is a Spark-based ETL application.

## Before-running
Zip reusable functions.
```sh
$ zip -r recipes-etl/common.zip recipes-etl/common
```

Install spark testing framework.
```sh
$ git clone https://github.com/holdenk/spark-testing-base.git
$ cd spark-testing-base
$ python setup.py build
$ python setup.py install
```


Export PYTHONPATH.
```sh
$ export PYTHONPATH=$PYTHONPATH:/your/path/to/recipes-etl
```

## Running

To run application locally.

```sh
$ spark-submit \
--master 'local[2]' \
--py-files recipes-etl/common.zip \
recipes-etl/main.py \
--ip_file /your/local/path/to/recipes.json \
--op_path /your/local/path/to/output_folder \
--edit_dist 2
```

To run application on yarn.

```sh
$ spark-submit \
--master yarn \
--py-files recipes-etl/common.zip \
recipes-etl/main.py \
--ip_file /your/hdfs/path/to/recipes.json \
--op_path /your/hdfs/path/to/output_folder \
--edit_dist 2
```

To run tests.

```sh
$ python -m recipes-etl.test.spark_test
```
## Reference

1. [Edit distance](http://norvig.com/spell-correct.html)
2. [Spark testing framework](https://github.com/holdenk/spark-testing-base)