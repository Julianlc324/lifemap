// LifeMap - CI/CD. Se dispara con cada merge a main (webhook de GitHub o pollSCM cada 2 min).
// Si cualquier prueba falla, el pipeline se detiene y la API pública NO se reemplaza.
pipeline {
  agent any
  options {
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '40'))
    timeout(time: 8, unit: 'HOURS')   // la primera corrida descarga y procesa el dataset completo
  }
  parameters {
    string(name: 'FORCE_STEPS', defaultValue: '',
           description: 'Pasos a repetir: ingest,life,sentiment,closure,balance,survival,risk o all')
  }
  environment {
    COMPOSE = 'docker compose -p lifemap -f docker-compose.yml'
    CORE = 'mongo spark-master spark-worker-1 spark-worker-2 dask-scheduler dask-worker-1 dask-worker-2 kafka'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
        script {
          env.GIT_COMMIT = sh(returnStdout: true, script: 'git rev-parse HEAD').trim()
          currentBuild.description = env.GIT_COMMIT.take(8)
        }
      }
    }

    stage('Build images') {
      steps {
        sh '$COMPOSE build --build-arg GIT_COMMIT=$GIT_COMMIT spark-master dask-scheduler'
      }
    }

    stage('Unit tests') {
      // Incluye tests/unit/test_metrics.py, que verifica la fórmula de la tasa de cierre
      steps {
        sh '$COMPOSE --profile test run --rm --no-deps -T tests -q tests/unit'
      }
    }

    stage('Start services') {
      steps {
        sh '$COMPOSE up -d --wait $CORE'
        sh '$COMPOSE up --no-deps kafka-init'
      }
    }

    stage('Data pipeline') {
      steps {
        withCredentials([usernamePassword(credentialsId: 'kaggle',
                                          usernameVariable: 'KAGGLE_USERNAME',
                                          passwordVariable: 'KAGGLE_KEY')]) {
          sh '$COMPOSE run --rm --no-deps -T -e KAGGLE_USERNAME -e KAGGLE_KEY -e FORCE_STEPS="$FORCE_STEPS" ingest'
        }
        sh '$COMPOSE run --rm --no-deps -T -e FORCE_STEPS="$FORCE_STEPS" batch'
      }
    }

    stage('API tests (candidate)') {
      steps {
        sh '$COMPOSE --profile test up -d --no-deps --wait api-test'
        sh '$COMPOSE --profile test run --rm --no-deps -T -e API_URL=http://api-test:5000 -e EXPECTED_COMMIT=$GIT_COMMIT tests -q tests/integration'
      }
      post {
        always { sh '$COMPOSE --profile test rm -sf api-test || true' }
      }
    }

    stage('Streaming tests') {
      when { expression { fileExists('streaming/stream_job.py') } }
      steps {
        sh '$COMPOSE up -d --no-deps streaming'
        // Modelo cargado = modelo versionado; humo: 100 mensajes -> clasificados en MongoDB
        sh '$COMPOSE --profile test run --rm --no-deps -T -e BUILD_TAG=$BUILD_TAG tests -q tests/smoke'
      }
    }

    stage('Deploy') {
      steps {
        sh '$COMPOSE up -d --no-deps api'
        sh '''
          for i in $(seq 1 30); do
            if $COMPOSE exec -T api curl -fs http://localhost:5000/ping | grep -q "$GIT_COMMIT"; then
              echo "Desplegado $GIT_COMMIT"; exit 0
            fi
            sleep 5
          done
          echo "La API no reporta el commit esperado" && exit 1
        '''
      }
    }
  }

  post {
    success { echo "LifeMap ${env.GIT_COMMIT} desplegado." }
    failure { echo 'Build fallido: no se desplegó la nueva versión (la API pública sigue en el commit anterior).' }
  }
}
