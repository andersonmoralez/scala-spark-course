ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-video-course",
    idePackagePrefix := Some("br.andersonmoralez.sparkvideocourse")
  )

// Versão Spark
val sparkVersion = "4.0.1"

// Dependências
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // Adicione outras bibliotecas do Spark conforme necessário (ex: spark-mllib, spark-streaming)

  // Dependência de Teste (Mantenha o ScalaTest na versão 3.2.18 ou superior)
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

// Opções para garantir que o código seja compilado corretamente com o Spark
scalacOptions += "-target:jvm-11" // Opcional: Define o bytecode para JVM 11 para maior compatibilidade do JAR, mas o Spark 4.x já é otimizado para JVM 17/21.

// Para usar o sbt run em modo local
fork := true