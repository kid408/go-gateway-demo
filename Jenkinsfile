pipeline {
  agent any

  options {
    skipDefaultCheckout(true)
  }

  environment {
    NOMAD_ADDR = 'http://127.0.0.1:4646'
    CONSUL_ADDR = 'http://127.0.0.1:8500'
    IMAGE_TAG = 'dev'
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
          set -eu
          docker version
          docker buildx version
          docker buildx ls
          nomad version
        '''
      }
    }

    stage('Preflight') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"

          echo '=== active nomad processes ==='
          ps -ef | grep '[n]omad' || true

          echo '=== consul leader ==='
          curl -fsS "${CONSUL_ADDR}/v1/status/leader"
          echo

          echo '=== nomad leader ==='
          curl -fsS "${NOMAD_ADDR}/v1/status/leader"
          echo

          echo '=== nomad node status ==='
          nomad node status

          READY_NODE="$(nomad node status -json | jq -r 'map(select(.Status=="ready"))[0].ID // empty')"
          test -n "${READY_NODE}"

          echo "=== ready node: ${READY_NODE} ==="
          nomad node status -verbose "${READY_NODE}" | tee /tmp/nomad-gateway-node.txt

          grep -Eq '^[[:space:]]*logs[[:space:]]' /tmp/nomad-gateway-node.txt
        '''
      }
    }

    stage('Build Image') {
      steps {
        sh '''
          set -eu
          docker buildx build -t go-gateway-demo:${IMAGE_TAG} . --load
        '''
      }
    }

    stage('Deploy') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"
          docker rm -f go-gateway-demo || true
          nomad job run -detach -var-file=nomad/gateway.vars.hcl nomad/gateway.nomad.hcl
        '''
      }
    }

    stage('Smoke Test') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"

          diagnose() {
            echo '=== nomad node status ==='
            nomad node status || true
            echo '=== nomad job status ==='
            nomad job status -verbose gateway || true
            echo '=== nomad job allocations ==='
            nomad job allocations gateway || true
            echo '=== consul gateway-http ==='
            curl -fsS "${CONSUL_ADDR}/v1/health/service/gateway-http?passing=true" | jq . || true
            echo '=== consul gateway-prom ==='
            curl -fsS "${CONSUL_ADDR}/v1/health/service/gateway-prom?passing=true" | jq . || true
          }

          trap 'diagnose' 0

          for _ in $(seq 1 30); do
            if curl -fsS "${CONSUL_ADDR}/v1/health/service/gateway-http?passing=true" | tee /tmp/gateway-http.json | jq -e 'length > 0' >/dev/null; then
              curl -fsS "${CONSUL_ADDR}/v1/health/service/gateway-prom?passing=true" | tee /tmp/gateway-prom.json | jq -e 'length > 0' >/dev/null
              nomad job status -verbose gateway
              jq . /tmp/gateway-http.json
              jq . /tmp/gateway-prom.json
              trap - 0
              exit 0
            fi

            sleep 2
          done

          exit 1
        '''
      }
    }
  }
}
