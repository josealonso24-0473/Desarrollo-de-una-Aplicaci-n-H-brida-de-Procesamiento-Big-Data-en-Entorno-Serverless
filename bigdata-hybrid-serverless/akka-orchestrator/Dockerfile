# Imagen de contenedor para el orquestador Akka (Etapa 3), desplegado en
# AWS Fargate detrás de API Gateway (HTTP API + VPC Link).
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY build.sbt .
COPY project ./project
COPY src ./src
RUN apt-get update && apt-get install -y curl && \
    curl -Ls https://github.com/sbt/sbt/releases/download/v1.10.1/sbt-1.10.1.tgz | tar xz && \
    ./sbt/bin/sbt stage

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/universal/stage /app
EXPOSE 8080
ENTRYPOINT ["/app/bin/bigdata-orchestrator"]
