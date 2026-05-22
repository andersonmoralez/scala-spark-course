package br.andersonmoralez.sparkvideocourse

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FirstTest extends AnyFunSuite {
  private val spark = SparkSession.builder()
    .appName("spark-video-course")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  private val inputSchema = StructType(Seq(
    StructField("Daily meals frequency", DoubleType, nullable =  true),
    StructField("Calories_Burned", DoubleType, nullable = true)
  ))

  private val outputSchema = StructType(Seq(
    StructField("Daily meals frequency", DoubleType, nullable =  true),
    StructField("Calories_Burned", DoubleType, nullable = true),
    StructField("rn", IntegerType, nullable = true)
  ))

  test("retorna somente registros únicos") {
    // Entradas de teste
    val testRows = Seq(
      Row(1.0, 200.0),
      Row(2.0, 100.0),
      Row(2.0, 200.0),
      Row(2.0, 300.0),
      Row(3.0, 400.0)
    )

    // Resultado esperado
    val testExpected = Seq(
      Row(1.0, 200.0, 1),
      Row(2.0, 300.0, 1),
      Row(3.0, 400.0, 1)
    )

    implicit val encoder: Encoder[Row] = Encoders.row(inputSchema)
    val testDf = spark.createDataset(testRows) // Cria o df

    // Processamento
    val actualRows = Main.mealFrequencyPerCalories(testDf)
      .collect()

    // Validação
    actualRows should contain theSameElementsAs testExpected
  }
}
