package br.andersonmoralez.sparkvideocourse

import io.DataLoader

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

    // 1. Defini o nome do arquivo que está na pasta 'data'
    val meuArquivoDeEstudo = "Final_data.csv"

    try {
      // 2. Carregando o arquivo
      val df: DataFrame = DataLoader.loadCsv(spark, meuArquivoDeEstudo)

      // 3. Log de carregamento
      if (AppConfig.printDebugInfo) {
        println("Dados carregados com sucesso!")
      }

      //df.printSchema() // Mostra a estrutura (colunas e tipos)
      df.show(10) // Mostra as 10 primeiras linhas



    } catch {
      case e: Exception =>
        println(s"Erro ao carregar o arquivo: $meuArquivoDeEstudo")
        println(s"Verifique se o arquivo existe em 'data/$meuArquivoDeEstudo'")
        e.printStackTrace()
    }

    println("Pressione ENTER para encerrar a aplicação...")
    scala.io.StdIn.readLine()

    spark.stop() // Boa prática: sempre pare a sessão no final
  }
}


