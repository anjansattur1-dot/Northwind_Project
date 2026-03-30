pipeline {
    agent any

    parameters {
        choice(
            name: 'LOAD_TYPE',
            choices: ['FULL_LOAD', 'INCREMENTAL_LOAD'],
            description: 'Select which pipeline to run'
        )
    }

    options {
        disableConcurrentBuilds()
        timestamps()
    }

    environment {
        SPARK_SUBMIT = '/usr/bin/spark-submit'
        HDFS_BASE = '/tmp/anjan_project'
    }

    stages {

        stage('Verify Workspace') {
            steps {
                sh '''
                    echo "=== VERIFY WORKSPACE ==="
                    pwd
                    ls -la

                    echo "Checking Spark:"
                    ${SPARK_SUBMIT} --version || true
                '''
            }
        }

        stage('Run Full Load') {
            when {
                expression { params.LOAD_TYPE == 'FULL_LOAD' }
            }
            steps {
                sh '''
                    echo "=== FULL LOAD STARTED ==="

                    ${SPARK_SUBMIT} --master yarn --jars postgresql-42.7.10.jar Full_Load_CSV_JAR.py
                    ${SPARK_SUBMIT} --master yarn silver_proj.py

                    echo "=== FULL LOAD COMPLETED ==="
                '''
            }
        }

        stage('Run Incremental Load') {
            when {
                expression { params.LOAD_TYPE == 'INCREMENTAL_LOAD' }
            }
            steps {
                sh '''
                    echo "=== INCREMENTAL LOAD STARTED ==="

                    nohup python3 kafka_prod_orders_stream.py > kafka_producer.log 2>&1 &
                    sleep 10

                    ${SPARK_SUBMIT} --master yarn kafka_consumer_order_stream.py
                    ${SPARK_SUBMIT} --master yarn orders_stream_bronze_to_silver.py
                    ${SPARK_SUBMIT} --master yarn orders_stream_hive.py

                    echo "=== INCREMENTAL LOAD COMPLETED ==="
                '''
            }
        }

        stage('Validate HDFS Output') {
            steps {
                sh '''
                    echo "=== VALIDATING HDFS ==="
                    hdfs dfs -ls ${HDFS_BASE}/bronze || true
                    hdfs dfs -ls ${HDFS_BASE}/silver || true
                    hdfs dfs -ls ${HDFS_BASE}/bronze/order_stream || true
                    hdfs dfs -ls ${HDFS_BASE}/silver/order_stream || true
                '''
            }
        }

        stage('Validate Hive') {
            steps {
                sh '''
                    echo "=== VALIDATING HIVE ==="
                    beeline -u jdbc:hive2://localhost:10000/default -e "
                    SHOW DATABASES;
                    SHOW TABLES;
                    " || true
                '''
            }
        }
    }

    post {
        success {
            echo 'PIPELINE SUCCESS'
        }
        failure {
            echo 'PIPELINE FAILED'
        }
    }
}
