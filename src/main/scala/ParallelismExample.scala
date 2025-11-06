package br.andersonmoralez.sparkvideocourse

import org.apache.spark.sql.SparkSession

object ParallelismExample {
  def main(args: Array[String]): Unit = {

    // 🔹 Cria a SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Parallelism Example")
      .master("local[*]") // usa todos os núcleos da máquina local
      .getOrCreate()

    // 🔹 Cria o contexto Spark (RDD API)
    val sc = spark.sparkContext

    // 🔹 Cria um RDD com 8 números e 4 partições
    val rdd = sc.parallelize(1 to 8, 4)

    println(s"Número de partições: ${rdd.getNumPartitions}")

    // 🔹 Exibe o conteúdo original de cada partição
    rdd.mapPartitionsWithIndex { (index, iterator) =>
      val data = iterator.toList
      println(s"Partição ${('A' + index).toChar} contém: ${data.mkString(", ")}")
      data.iterator
    }.collect()

    println("\n--- Iniciando processamento paralelo ---\n")

    // 🔹 Processa cada partição e mostra o thread responsável
    val processed = rdd.mapPartitionsWithIndex { (index, iterator) =>
      val data = iterator.toList
      val threadName = Thread.currentThread().getName
      println(
        s"Processando partição ${('A' + index).toChar} com valores ${data.mkString(", ")} " +
          s"no thread: $threadName"
      )
      // Simula o processamento (dobra os valores)
      data.map(_ * 2).iterator
    }

    val result = processed.collect()

    println("\n--- Resultado Final ---")
    println(result.mkString(", "))

    spark.stop()
  }
}