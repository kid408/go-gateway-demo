pipeline {
  agent any

  options {
    skipDefaultCheckout(true)
  }

  environment {
    HOST_APP_PORT = '38080'
    HOST_METRICS_PORT = '32112'
    HOST_LOG_DIR = '/opt/monitoring/fluent-bit/logs/gateway'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Check Docker') {
      steps {
        sh '''
          docker version
          docker buildx version
          docker buildx ls
        '''
      }
    }

    stage('Build Image') {
      steps {
        sh '''
          docker buildx build -t go-gateway-demo:latest . --load
        '''
      }
    }

    stage('Deploy') {
      steps {
        sh '''
          docker rm -f go-gateway-demo || true
          mkdir -p ${HOST_LOG_DIR}
          docker run -d \
            --name go-gateway-demo \
            --restart unless-stopped \
            --add-host=host.docker.internal:host-gateway \
            -e SERVICE_NAME=gateway \
            -e TARGET_SERVICE_NAME=worker \
            -e TARGET_DISCOVERY_SERVICE_NAME=worker-http \
            -e APP_PORT=18080 \
            -e METRICS_PORT=12112 \
            -e CONSUL_HTTP_ADDR=http://host.docker.internal:8500 \
            -e APP_LOG_PATH=/app/logs/go-gateway-demo.log \
            -p ${HOST_APP_PORT}:18080 \
            -p ${HOST_METRICS_PORT}:12112 \
            -v ${HOST_LOG_DIR}:/app/logs \
            go-gateway-demo:latest
        '''
      }
    }

    stage('Smoke Test') {
      steps {
        sh '''
          sleep 5
          curl -fsS http://127.0.0.1:${HOST_APP_PORT}/healthz
          curl -fsS http://127.0.0.1:${HOST_APP_PORT}/workers
          curl -fsS http://127.0.0.1:${HOST_METRICS_PORT}/metrics | grep '^go_gateway_process_up'
          curl -fsS http://127.0.0.1:${HOST_METRICS_PORT}/metrics | grep '^go_gateway_online_users'
        '''
      }
    }
  }
}

