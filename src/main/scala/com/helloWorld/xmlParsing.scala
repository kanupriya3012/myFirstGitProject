package com.helloWorld
import org.apache.spark.sql._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object xmlParsing {

  def flatten_xml(input_Dataframe: DataFrame) = {

    println("{PRINT} : Inside flattenxml method")
    var xmlDataframe:DataFrame=input_Dataframe
    var selectColumnList=List.empty[String]

    for (columnName<-xmlDataframe.schema.names ){
      if(xmlDataframe.schema(columnName).dataType.isInstanceOf[ArrayType]){
        xmlDataframe=xmlDataframe.withColumn(columnName,explode(xmlDataframe(columnName)).alias(columnName))
        selectColumnList:+=columnName
        println("check if 2 cols in array ")
        xmlDataframe.show(2)
      }

      if(xmlDataframe.schema(columnName).dataType.isInstanceOf[StructType]){
        //println(f"{PRINT} flatenning struct column $columnName")
        //println(xmlDataframe.schema(columnName).dataType.asInstanceOf[StructType])
        for (column<-xmlDataframe.schema(columnName).dataType.asInstanceOf[StructType].fields){
          selectColumnList:+=columnName+"."+column.name

        }
      }
      else{
        selectColumnList:+=columnName
      }

    }
    println("Select column List "+selectColumnList)
    val mapColumnList=selectColumnList.map(name=>col(name).alias(name.replace(".","_")))
    println("map="+mapColumnList)
    //xmlDataframe.select(mapColumnList:_*).show()

  }

  def main(args: Array[String]): Unit = {
    println("{PRINT}:Starting my application..")

    val spark = SparkSession.builder()
      .appName("Create DataFrame from XML File")
      .master("local[*]")
      .getOrCreate()

    val sc=spark.sparkContext

    //logger
    import org.apache.log4j.{Level,Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)

    sc.setLogLevel("ERROR")

    var column_list=List.empty[String]

    //Code Block 1 Starts Here
    println("{PRINT}:Reading xml dataframe..")


    val xml_file_path = "C:\\Kanu\\xmlFile\\sampleXml.xml"
    var xml_df = spark.read.option("rowTag", "feeds").xml(xml_file_path)
    xml_df.printSchema()
    xml_df.show(5)
    println("{PRINT}:Reading xml dataframe completed")

    var xml_counter=1
    //while(xml_counter!=0){
      println("{PRINT}:Inside column counter")
      for(column_name<-xml_df.schema.names){

        val column_type=xml_df.schema(column_name)
        //println(f"{PRINT}: $column_name = $column_type")

        if(xml_df.schema(column_name).dataType.isInstanceOf[StructType]
          || xml_df.schema(column_name).dataType.isInstanceOf[ArrayType]
        )
        {
          //println(f"{PRINT} Flatten this column")
          column_list:+=column_name
          flatten_xml(xml_df)
        }

      }
    println("{PRINT : columns to be flattened}")
    println(column_list)
    }
  //}

}
