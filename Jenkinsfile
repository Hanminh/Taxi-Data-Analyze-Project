pipeline {
    agent any
    stages {
        stage('Start Hadoop'){
            agent { label 'hadoop' }
            steps{
                script {
                    sh '''
                    if [ -f /tmp/hadoop-hdoop-namenode.pid ] && [ -f /tmp/hadoop-hdoop-datanode.pid ] && [ -f /tmp/hadoop-hdoop-secondarynamenode.pid ]; then
                      echo "Namenode - Database - Secondary Secondary is running..."
                    else
                      /home/hdoop/hadoop-3.3.6/sbin/start-dfs.sh
                    fi
                    
                    if [ -f /tmp/hadoop-hdoop-resourcemanager.pid ] && [ -f /tmp/hadoop-hdoop-nodemanager.pid ]; then
                      echo "ResourceManager - NodeManager is running..."
                    else
                      /home/hdoop/hadoop-3.3.6/sbin/start-yarn.sh
                    fi
                    
                    jps
                    '''
                }
            }
            post {
                success {
                    echo '--- Start Hadoop Success ---'
                }
            }
        }

        stage('Create Database Cassandra') {
            steps {
                script{
                    sh '''
                    python3 /opt/BigData-IT4931/Cassandra_Execute/create_table_1.py
                    python3 /opt/ubuntu/BigData-IT4931/Cassandra_Execute/create_table_2.py
                    '''
                }
            }
            post {
                success {
                    echo '--- Create Database Success ---'
                }
            }
        }

        stage('Transformation') {
            agent { label 'spark' }
            steps {
                script{
                    sh '''
                    . /home/ubuntu/myenv/bin/activate
                    python /home/ubuntu/spark/passenger_statistic.py
                    python /home/ubuntu/spark/payment_type_statistic.py
                    python /home/ubuntu/spark/peak_pickup_hour_statistic.py
                    python /home/ubuntu/spark/total_amount_per_day_statistic.py
                    deactivate
                    '''
                }
            }
            post {
                success {
                    echo '--- Transformation Success ---'
                }
            }
        }
    }
}