package br.andersonmoralez.sparkvideocourse

import io.DataLoader

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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

      // df.printSchema() // Mostra a estrutura (colunas e tipos)
      // df.show(10) // Mostra as 10 primeiras linhas

      import spark.implicits._

      // Agrupa por tipo de atividade física
      df
        .groupBy($"Workout_Type".as("workoutType"))
        .agg(
          functions.avg($"Age").as("avgAge"), // Média de idade
          functions.max($"Weight (kg)").as("maxWeightKg"), // Maior peso
          functions.min($"Weight (kg)").as("minWeightKg") // Menor peso
        )
        .sort($"workoutType".desc)
        .show()

      // Agrupa por atividade física
      df
        .groupBy($"Workout_Type".as("workoutType"))
        .max("Weight (kg)", "Height (m)") // Maior peso e altura
        // .show()

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


