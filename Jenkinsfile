pipeline {
  agent any

  options {
    skipDefaultCheckout(true)
  }

  environment {
    NOMAD_ADDR = 'http://127.0.0.1:4646'
    CONSUL_ADDR = 'http://127.0.0.1:8500'
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
          nomad version
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
          export NOMAD_ADDR=${NOMAD_ADDR}
          docker rm -f go-gateway-demo || true
          nomad job run -detach -var-file=nomad/gateway.vars.hcl nomad/gateway.nomad.hcl
        '''
      }
    }

    stage('Smoke Test') {
      steps {
        sh '''
          export NOMAD_ADDR=${NOMAD_ADDR}
          sleep 10
          nomad node status
          nomad job status -verbose gateway
          curl -fsS ${CONSUL_ADDR}/v1/health/service/gateway-http?passing=true | tee /tmp/gateway-http.json | jq .
          jq -e 'length > 0' /tmp/gateway-http.json >/dev/null
          curl -fsS ${CONSUL_ADDR}/v1/health/service/gateway-prom?passing=true | tee /tmp/gateway-prom.json | jq .
          jq -e 'length > 0' /tmp/gateway-prom.json >/dev/null
        '''
      }
    }
  }
}
