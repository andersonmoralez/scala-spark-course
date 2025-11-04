package br.andersonmoralez.sparkvideocourse.io

import br.andersonmoralez.sparkvideocourse.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Um objeto utilitário para carregar dados de arquivos no projeto.
 */
object DataLoader {

  /**
   * Carrega um arquivo CSV do diretório 'data' do projeto.
   *
   * @param spark A SparkSession ativa.
   * @param fileName O nome do arquivo (ex: "meus_dados.csv").
   * @return Um DataFrame do Spark com os dados.
   */
  def loadCsv(spark: SparkSession, fileName: String): DataFrame = {
    // O caminho é relativo à raiz do projeto
    val filePath = s"data/$fileName"

    if (AppConfig.printDebugInfo) {
      println(s"Carregando dados do arquivo: $filePath")
    }

    spark.read
      .option("header", "true") // Assume que a primeira linha é o cabeçalho
      .option("inferSchema", "true") // Tenta adivinhar o tipo de cada coluna
      .csv(filePath)
  }

  /**
   * Você pode adicionar outros leitores aqui (JSON, Parquet, etc.)
   *
   * def loadJson(spark: SparkSession, fileName: String): DataFrame = {
   * val filePath = s"data/$fileName"
   * spark.read.json(filePath)
   * }
   */

}