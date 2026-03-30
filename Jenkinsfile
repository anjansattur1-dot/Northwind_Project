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
        PROJECT_HOME = '/home/ec2-user/250226batch/anjan/northwind_project'
        HDFS_BASE = '/tmp/anjan_project'
    }

    stages {

        stage('Verify Workspace') {
            steps {
                sh '''
                    echo "=== VERIFY WORKSPACE ==="
                    pwd
                    ls -la
                    ls -la ${PROJECT_HOME} || true
                    ls -la ${PROJECT_HOME}/scripts || true
                    ls -la ${PROJECT_HOME}/kafka || true
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

                    cd ${PROJECT_HOME}

                    echo "Running bronze load from SQL DB..."
                    spark-submit --master yarn scripts/Full_Load_CSV_JAR.py

                    echo "Running silver cleaning..."
                    spark-submit --master yarn scripts/silver_proj.py

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

                    cd ${PROJECT_HOME}

                    echo "Starting Kafka producer in background..."
                    nohup python3 kafka/kafka_prod_orders_stream.py > kafka_producer.log 2>&1 &

                    sleep 10

                    echo "Running Kafka consumer..."
                    spark-submit --master yarn kafka/kafka_consumer_order_stream.py

                    echo "Moving streamed bronze to silver..."
                    spark-submit --master yarn kafka/orders_stream_bronze_to_silver.py

                    echo "Loading/refreshing Hive-Impala output..."
                    spark-submit --master yarn kafka/orders_stream_hive.py

                    echo "=== INCREMENTAL LOAD COMPLETED ==="
                '''
            }
        }

        stage('Validate HDFS Output') {
            steps {
                sh '''
                    echo "=== VALIDATING HDFS ==="

                    echo "Bronze root:"
                    hdfs dfs -ls ${HDFS_BASE}/bronze || true

                    echo "Silver root:"
                    hdfs dfs -ls ${HDFS_BASE}/silver || true

                    echo "Bronze order_stream:"
                    hdfs dfs -ls ${HDFS_BASE}/bronze/order_stream || true

                    echo "Silver order_stream:"
                    hdfs dfs -ls ${HDFS_BASE}/silver/order_stream || true
                '''
            }
        }

        stage('Validate Hive') {
            steps {
                sh '''
                    echo "=== VALIDATING HIVE / IMPALA TABLES ==="
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
        always {
            sh '''
                echo "=== FINAL CHECK ==="
                date
            '''
        }
    }
}
