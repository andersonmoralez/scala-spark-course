package br.andersonmoralez.sparkvideocourse

import br.andersonmoralez.sparkvideocourse.io.DataLoader
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


      //df.printSchema() // Mostra a estrutura (colunas e tipos)
      //df.show(10) // Mostra as 10 primeiras linhas

      //df.select("Age", "Gender", "Avg_BPM").show()
      //val column = df("Age")
      col("Age")
      import spark.implicits._
      $"Age"

      //df.select(column, $"Gender", df("Avg_BPM")).show()

      val column = df("Age")
      val newColumn = (column + 2.0).as("Age_2.0")
      val columnString = column.cast(StringType).as("Age_String_Type")

      df.select(column, newColumn, columnString)
        .filter(newColumn > 20.00)
        .filter(newColumn > column)
        .show()

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


