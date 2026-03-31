pipeline {
    agent any

    parameters {
        choice(
            name: 'RUN_MODE',
            choices: ['FULL_LOAD', 'INCREMENTAL_LOAD'],
            description: 'Choose which ETL mode to run'
        )
    }

    environment {
        CLOUDERA_HOST = 'ec2-user@13.41.167.97'
        SSH_KEY       = '/var/lib/jenkins/.ssh/id_rsa'
        REMOTE_DIR    = '/tmp/anjan_northwind_pipeline'
        PYTHON_BIN    = 'python3'
        PIP_BIN       = 'pip3'
        JDBC_JAR      = 'postgresql-42.7.10.jar'
    }

    options {
        disableConcurrentBuilds()
        timestamps()
    }

    stages {

        stage('Checkout Source') {
            steps {
                checkout scm
            }
        }

        stage('Validate Jenkins Workspace') {
            steps {
                sh '''
                    echo "=== JENKINS WORKSPACE ==="
                    pwd
                    ls -la
                    echo "=== TEST FILES ==="
                    ls -la tests || true
                '''
            }
        }

        stage('Validate Cluster Health') {
            steps {
                sh '''
                    echo "=== CHECKING CLOUDERA CLUSTER ==="
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        echo 'Hostname:' && hostname
                        echo 'Spark:' && which spark-submit
                        echo 'Python:' && which ${PYTHON_BIN}
                        echo 'Pip:' && which ${PIP_BIN}
                        echo 'YARN nodes:' && HADOOP_CONF_DIR=/etc/hadoop/conf yarn node -list 2>&1 | grep -E 'Total|RUNNING'
                        echo 'HDFS report:' && hdfs dfsadmin -report 2>&1 | grep -E 'Configured|DFS Remaining'
                    "
                '''
            }
        }

        stage('Fetch and Upload JDBC Driver') {
            steps {
                sh '''
                    echo "=== FETCHING JDBC DRIVER ==="

                    rm -f ${JDBC_JAR}
                    wget https://jdbc.postgresql.org/download/postgresql-42.7.10.jar -O ${JDBC_JAR}

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        rm -rf ${REMOTE_DIR}
                        mkdir -p ${REMOTE_DIR}/tests
                    "

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        ${JDBC_JAR} \
                        ${CLOUDERA_HOST}:${REMOTE_DIR}/
                '''
            }
        }

        stage('Copy Project Scripts and Tests to Cluster') {
            steps {
                sh '''
                    echo "=== COPYING PROJECT FILES TO CLUSTER ==="

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        Full_Load_CSV_JAR.py \
                        silver_proj.py \
                        silver_to_gold.py \
                        logger_util.py \
                        kafka_prod_orders_stream.py \
                        kafka_consumer_order_stream.py \
                        orders_stream_bronze_to_silver.py \
                        orders_stream_hive.py \
                        requirements.txt \
                        ${CLOUDERA_HOST}:${REMOTE_DIR}/

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        tests/test_floadtest.py \
                        tests/test_cleaning.py \
                        tests/test_transformation.py \
                        ${CLOUDERA_HOST}:${REMOTE_DIR}/tests/
                '''
            }
        }

        stage('Install Python Dependencies') {
            steps {
                sh '''
                    echo "=== INSTALLING PYTHON DEPENDENCIES ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        set -e
                        cd ${REMOTE_DIR}

                        ${PYTHON_BIN} -m pip install --upgrade pip
                        ${PYTHON_BIN} -m pip install pytest pyspark
                    "
                '''
            }
        }

        stage('Run Pytests') {
            when {
                expression { params.RUN_MODE == 'FULL_LOAD' }
            }
            steps {
                sh '''
                    echo "=== RUNNING PYTESTS FIRST ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        set -e
                        cd ${REMOTE_DIR}

                        ${PYTHON_BIN} -m pytest -v tests/test_floadtest.py
                        ${PYTHON_BIN} -m pytest -v tests/test_cleaning.py
                        ${PYTHON_BIN} -m pytest -v tests/test_transformation.py
                    "
                '''
            }
        }

        stage('Run Full Load') {
            when {
                expression { params.RUN_MODE == 'FULL_LOAD' }
            }
            steps {
                sh '''
                    echo "=== RUNNING FULL LOAD ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        set -e
                        cd ${REMOTE_DIR}

                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf
                        export HIVE_CONF_DIR=/etc/hive/conf

                        echo '--- Full Load: PostgreSQL to Bronze ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --jars ${REMOTE_DIR}/${JDBC_JAR} \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/Full_Load_CSV_JAR.py

                        echo '--- Silver Cleaning ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/silver_proj.py

                        echo '--- Gold Transformation ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/silver_to_gold.py
                    "
                '''
            }
        }

        stage('Run Incremental Load') {
            when {
                expression { params.RUN_MODE == 'INCREMENTAL_LOAD' }
            }
            steps {
                sh '''
                    echo "=== RUNNING INCREMENTAL LOAD ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        set -e
                        cd ${REMOTE_DIR}

                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf
                        export HIVE_CONF_DIR=/etc/hive/conf

                        echo '--- Starting Kafka Producer in Background ---'
                        nohup ${PYTHON_BIN} ${REMOTE_DIR}/kafka_prod_orders_stream.py > ${REMOTE_DIR}/kafka_producer.log 2>&1 &
                        sleep 10

                        echo '--- Running Kafka Consumer ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/kafka_consumer_order_stream.py

                        echo '--- Bronze to Silver for Streamed Orders ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/orders_stream_bronze_to_silver.py

                        echo '--- Loading Stream Output to Hive / Impala ---'
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --py-files ${REMOTE_DIR}/logger_util.py \
                          ${REMOTE_DIR}/orders_stream_hive.py
                    "
                '''
            }
        }

        stage('Validate HDFS Output') {
            steps {
                sh '''
                    echo "=== VALIDATING HDFS OUTPUT ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        hdfs dfs -ls /tmp/anjan_project/bronze || true
                        hdfs dfs -ls /tmp/anjan_project/silver || true
                        hdfs dfs -ls /tmp/anjan_project/bronze/orders_stream || true
                        hdfs dfs -ls /tmp/anjan_project/silver/orders_stream || true
                    "
                '''
            }
        }

        stage('Invalidate Impala Metadata') {
            steps {
                sh '''
                    echo "=== INVALIDATING IMPALA METADATA ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        impala-shell -q 'INVALIDATE METADATA;'
                    " || true
                '''
            }
        }

        stage('Validate Hive / Impala') {
            steps {
                sh '''
                    echo "=== VALIDATING HIVE / IMPALA ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        beeline -u jdbc:hive2://localhost:10000/default -e '
                        SHOW DATABASES;
                        SHOW TABLES;
                        '
                    " || true
                '''
            }
        }

        stage('Check Recent YARN Apps') {
            steps {
                sh '''
                    echo "=== RECENT YARN APPLICATIONS ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        yarn application -list -appStates FINISHED | head -20
                    " || true
                '''
            }
        }
    }

    post {
        success {
            echo 'Pytests passed and ETL pipeline completed successfully.'
        }
        failure {
            echo 'Pipeline failed. Check pytest, Spark, and Jenkins console logs.'
        }
    }
}
