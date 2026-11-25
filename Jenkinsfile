pipeline {
    agent any

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    environment {
        SOLUTION      = 'HotelBookingSystem.slnx'
        SERVER_PROJECT = 'Server/Server.csproj'
        TESTS_PROJECT  = 'Tests/Tests.csproj'
        IMAGE_NAME     = 'riot-server'
        IMAGE_TAG      = "${env.BUILD_NUMBER}"
        DOCKERFILE     = 'Server/Dockerfile'
        DEPLOYMENT     = 'riot-server'
        CONTAINER      = 'server'
        NAMESPACE      = 'default'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh '''
                    echo "=== Checkout ==="
                    git rev-parse --abbrev-ref HEAD
                    git rev-parse HEAD
                    git log -1 --oneline
                '''
            }
        }

        stage('Restore') {
            steps {
                sh '''
                    echo "=== Restore ==="
                    dotnet --version
                    dotnet restore "${SOLUTION}"
                '''
            }
        }

        stage('Build') {
            steps {
                sh '''
                    echo "=== Build (Release) ==="
                    dotnet build "${SOLUTION}" --configuration Release --no-restore
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                    echo "=== Test ==="
                    rm -rf TestResults
                    dotnet test "${TESTS_PROJECT}" \
                        --configuration Release \
                        --no-build \
                        --results-directory TestResults \
                        --logger "trx;LogFileName=lr10.trx"
                '''
            }
            post {
                always {
                    archiveArtifacts artifacts: 'TestResults/**/*.trx', allowEmptyArchive: true
                }
            }
        }

        stage('Package') {
            steps {
                sh '''
                    echo "=== Package ==="
                    docker build \
                        -f "${DOCKERFILE}" \
                        -t "${IMAGE_NAME}:${IMAGE_TAG}" \
                        .
                    docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" \
                        --format 'IMAGE={{.RepoTags}} ID={{.Id}} SIZE={{.Size}}'
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    echo "=== Deploy ==="
                    kubectl config current-context
                    kubectl config set-context --current --namespace="${NAMESPACE}"

                    kubectl set image "deployment/${DEPLOYMENT}" \
                        "${CONTAINER}=${IMAGE_NAME}:${IMAGE_TAG}" \
                        --namespace="${NAMESPACE}"

                    kubectl rollout status "deployment/${DEPLOYMENT}" \
                        --namespace="${NAMESPACE}" \
                        --timeout=180s

                    kubectl get deployment "${DEPLOYMENT}" --namespace="${NAMESPACE}"
                    kubectl get pods -l app="${DEPLOYMENT}" --namespace="${NAMESPACE}" -o wide
                '''
            }
        }
    }

    post {
        success {
            echo "Pipeline ${env.JOB_NAME} #${env.BUILD_NUMBER}: SUCCESS. Образ ${IMAGE_NAME}:${IMAGE_TAG} развёрнут в Kubernetes."
        }
        failure {
            echo "Pipeline ${env.JOB_NAME} #${env.BUILD_NUMBER}: FAILED. Следующие этапы не выполнялись. См. Console Output."
        }
        always {
            sh 'docker images "${IMAGE_NAME}" --format "{{.Repository}}:{{.Tag}} {{.ID}} {{.Size}}" || true'
        }
    }
}
