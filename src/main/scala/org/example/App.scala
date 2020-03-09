package org.example

import org.apache.spark.sql.SparkSession
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val spark = SparkSession.builder
      .appName("Deequ")
      .master("local[*]")
      .getOrCreate()

    val dF = spark.read
      .option("header", true)
      .csv("/home/adnls/data/Titanic.csv")

    dF.printSchema()

    val verificationResult = VerificationSuite()
      .onData(dF)
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .hasSize(_ > 0)
          .isComplete("Survived")
          .isUnique("Name")
          .isComplete("Age")
          .isContainedIn("Sex", Array("male", "female"))
          .isNonNegative("Survived"))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }
  }
}
