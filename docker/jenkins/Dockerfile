# Jenkins con Docker CLI + Compose. Usa el socket de Docker del servidor (montado en compose)
# para construir imágenes y desplegar el mismo proyecto Compose "lifemap".
FROM jenkins/jenkins:lts-jdk17

USER root
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && echo "deb [signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list \
 && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

COPY docker/jenkins/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

# Configuración como código: usuario admin, credenciales (desde variables de entorno) y el job
COPY docker/jenkins/casc.yaml /var/jenkins_conf/casc.yaml
ENV CASC_JENKINS_CONFIG=/var/jenkins_conf/casc.yaml \
    JAVA_OPTS="-Djenkins.install.runSetupWizard=false"
# Se ejecuta como root para poder usar /var/run/docker.sock (documentado en el README)
USER root
