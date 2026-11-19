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
    string(name: 'FORCE_STEPS', defaultValue: 'survival,risk,balance',
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
          env.IMAGE_TAG = env.GIT_COMMIT
          env.SOURCE_DATABASE = sh(returnStdout: true, script: "docker exec lifemap-api-1 sh -c 'printenv MONGO_DB || echo lifemap' 2>/dev/null || echo lifemap").trim()
          env.SOURCE_MODELS_DIR = sh(returnStdout: true, script: "docker exec lifemap-api-1 sh -c 'printenv MODELS_DIR || echo /models' 2>/dev/null || echo /models").trim()
          env.SOURCE_DATA_DIR = sh(returnStdout: true, script: "docker exec lifemap-api-1 sh -c 'printenv DATA_DIR || echo /data' 2>/dev/null || echo /data").trim()
          env.SOURCE_CHECKPOINT_DIR = sh(returnStdout: true, script: "docker exec lifemap-streaming-1 sh -c 'printenv CHECKPOINT_DIR || echo /checkpoints/v2' 2>/dev/null || echo /checkpoints/v2").trim()
          env.MONGO_DB = "lifemap_candidate_${env.GIT_COMMIT.take(12)}_${env.BUILD_NUMBER}"
          env.MODELS_DIR = "/models/releases/${env.MONGO_DB}"
          env.DATA_DIR = "/data/releases/${env.MONGO_DB}"
          env.CHECKPOINT_DIR = "/checkpoints/releases/${env.MONGO_DB}"
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
        sh '$COMPOSE up -d --no-recreate --wait $CORE'
        sh '$COMPOSE up --no-deps kafka-init'
      }
    }

    stage('Prepare isolated candidate') {
      steps {
        sh """
          was_running=\$(docker inspect -f '{{.State.Running}}' lifemap-streaming-1 2>/dev/null || true)
          if [ "\$was_running" = true ]; then docker stop -t 60 lifemap-streaming-1; fi
          trap 'if [ "\$was_running" = true ]; then docker start lifemap-streaming-1; fi' EXIT
          \$COMPOSE run --rm --no-deps -T -e SOURCE_DATABASE -e SOURCE_MODELS_DIR -e SOURCE_DATA_DIR -e SOURCE_CHECKPOINT_DIR jobs python3 bin/prepare_candidate.py
        """
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
      steps {
        // Modelo cargado = modelo versionado; humo: 100 mensajes -> clasificados en MongoDB
        sh '$COMPOSE --profile test run --rm --no-deps -T -e RUN_STREAM_SMOKE=1 tests -q tests/smoke'
      }
    }

    stage('Deploy') {
      steps {
        sh '$COMPOSE up -d --no-deps api streaming'
        sh '''
          for i in $(seq 1 30); do
            if $COMPOSE exec -T api python3 bin/check_health.py; then
              echo "Desplegado $GIT_COMMIT"
              printf '%s\n' "IMAGE_TAG=$IMAGE_TAG" "GIT_COMMIT=$GIT_COMMIT" "MONGO_DB=$MONGO_DB" "MODELS_DIR=$MODELS_DIR" "DATA_DIR=$DATA_DIR" "CHECKPOINT_DIR=$CHECKPOINT_DIR" > /var/jenkins_home/lifemap-active.env
              exit 0
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
    failure { echo 'Build fallido. Consultar la etapa: no se promueve si fallan pruebas; verificar /health si falló Deploy.' }
  }
}
