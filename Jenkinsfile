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

    stages {

        stage('Verify Workspace') {
            steps {
                sh '''
                    echo "=== VERIFY WORKSPACE ==="
                    pwd
                    ls -la
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

                    spark-submit --master yarn Full_Load_CSV_JAR.py
                    spark-submit --master yarn silver_proj.py

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

                    spark-submit --master yarn kafka_consumer_order_stream.py
                    spark-submit --master yarn orders_stream_bronze_to_silver.py
                    spark-submit --master yarn orders_stream_hive.py

                    echo "=== INCREMENTAL LOAD COMPLETED ==="
                '''
            }
        }

        stage('Validate HDFS Output') {
            steps {
                sh '''
                    echo "=== VALIDATING HDFS ==="
                    hdfs dfs -ls /tmp/anjan_project/bronze || true
                    hdfs dfs -ls /tmp/anjan_project/silver || true
                    hdfs dfs -ls /tmp/anjan_project/bronze/order_stream || true
                    hdfs dfs -ls /tmp/anjan_project/silver/order_stream || true
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
