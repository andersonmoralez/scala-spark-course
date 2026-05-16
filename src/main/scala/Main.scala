package br.andersonmoralez.sparkvideocourse

import io.DataLoader

import org.apache.spark.sql.functions.{col, concat, current_timestamp, expr, lit}
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

    // Definir o nome do arquivo que está na pasta 'data'
    val meuArquivoDeEstudo = "Final_data.csv"

    try {
      // val df = DataLoader.loadCsv(spark, meuArquivoDeEstudo)
      val df: DataFrame = DataLoader.loadCsv(spark, meuArquivoDeEstudo)

      // Mostrar os dados
      if (AppConfig.printDebugInfo) {
        println("Dados carregados com sucesso!")
      }


      //df.printSchema() // Mostra a estrutura (colunas e tipos)
//      df.show(10) // Mostra as 10 primeiras linhas

//      val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampExpression") // Expression
//      val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFunctions") // Functions
//      df.select(timestampFromExpression, timestampFromFunctions).show()

//      df.selectExpr("cast(Age as string)", "Age + 1.0", "current_timestamp()").show() // Select Expression

      // Select Functions
      df.createTempView("df")
      spark.sql("select cast(Age as string) , Age + 1.0, current_timestamp() from df").show()

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


