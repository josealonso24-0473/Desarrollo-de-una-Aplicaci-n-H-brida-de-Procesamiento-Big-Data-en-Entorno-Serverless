name := "bigdata-orchestrator"
version := "1.0.0"
scalaVersion := "2.13.14"

val AkkaVersion = "2.9.3"
val AkkaHttpVersion = "10.6.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed"   % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"        % AkkaVersion,
  "com.typesafe.akka" %% "akka-http"          % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-slf4j"         % AkkaVersion,
  "ch.qos.logback"     % "logback-classic"    % "1.5.6",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "org.scalatest"     %% "scalatest"          % "3.2.19"        % Test
)

// Empaquetado como imagen de contenedor para despliegue en AWS Fargate
enablePlugins(JavaAppPackaging)
