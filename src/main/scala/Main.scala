package br.andersonmoralez.sparkvideocourse

import io.DataLoader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-video-course")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // 2. Definir o nome do arquivo que está na pasta 'data'
    val meuArquivoDeEstudo = "Final_data.csv"

    try {
      // val df = DataLoader.loadCsv(spark, meuArquivoDeEstudo)

      val df: DataFrame = DataLoader.loadCsv(spark, meuArquivoDeEstudo)

      // 4. Mostrar os dados
      if (AppConfig.printDebugInfo) {
        println("Dados carregados com sucesso!")
      }

      // df.show()
      // df.printSchema()

      // Renomeando colunas
      val renameColumns = List(
        col("Age").as("age"),
        col("Gender").as("gender"),
        col("Weight (kg)").as("weightKg"),
        col("Height (m)").as("heightM"),
        col("Max_BPM").as("maxBPM"),
        col("Avg_BPM").as("avgBPM")
      )

      // df.select(renameColumns: _*).show()

      // Diferença entre dois valores
      val weightHeight = df.select(renameColumns: _*)
        .withColumn("diff", col("weightKg") - col("heightM"))
        .filter(col("weightKg") > col("heightM")) // heightM deve ser maior que WeightKg

      weightHeight.show()

    } catch {
      case e: Exception =>
        println(s"Erro ao carregar o arquivo: $meuArquivoDeEstudo")
        println(s"Verifique se o arquivo existe em 'data/$meuArquivoDeEstudo'")
        e.printStackTrace()
    }

    // Adicione esta linha para pausar e verificar a saída antes do shutdown
    println("Pressione ENTER para encerrar a aplicação...")
    scala.io.StdIn.readLine()

    spark.stop() // Boa prática: sempre pare a sessão no final
  }
}


