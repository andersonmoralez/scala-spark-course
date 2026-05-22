package br.andersonmoralez.sparkvideocourse

import io.DataLoader

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
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
      //df.show(10) // Mostra as 10 primeiras linhas

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

  // Método da Window Functions
  def mealFrequencyPerCalories(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._ // Importa recusos implicitos a partir da sessão anterior

    //Particiona por Frequência de Refeição Diária e ordena por Calorias Perdidas
    val window = Window.partitionBy($"Daily meals frequency".as("dailyMealsFrequency")).orderBy($"Calories_Burned".desc)
    df.withColumn("rn", row_number.over(window))
      .filter($"rn" === 1) // Filtra frequência única
      .sort($"Calories_Burned".desc) // Ordena pela maior caloria perdida
  }
}


