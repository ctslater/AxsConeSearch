
package axs

import scala.math
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.hadoop.fs.Path

import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate

import  uk.ac.starlink.table.{StarTable, ColumnInfo, RowListStarTable}
import uk.ac.starlink.votable.VOTableWriter

import org.rogach.scallop._

import Ordering.Implicits._

class AxsConeConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val filename = trailArg[String]()
  verify()
}

object ExitLoop extends Exception { }

object AxsConeMain {

  def main(args: Array[String]) {
    val conf = new AxsConeConf(args)
    val cone_radius = 5/3600.0
    val center_ra = 287.9869104
    val center_dec = 13.0748496
  
    // println(rec)
    val outputInfo : Array[ColumnInfo] = Array.ofDim(3)
    outputInfo(0) = new ColumnInfo("matchid", classOf[java.lang.Long], "Object ID")
    outputInfo(1) = new ColumnInfo("ra", classOf[java.lang.Double], "RA")
    outputInfo(2) = new ColumnInfo("dec", classOf[java.lang.Double], "Dec")
    /* 
    outputInfo(3) = new ColumnInfo("mjd", classOf[java.lang.Double], "MJD")
    outputInfo(4) = new ColumnInfo("mag", classOf[java.lang.Double], "Mag")
    outputInfo(5) = new ColumnInfo("magerr", classOf[java.lang.Double], "Mag Error")
    outputInfo(6) = new ColumnInfo("filterid", classOf[java.lang.Integer], "Filter (1:g, 2:R, 3:i)")
    outputInfo(7) = new ColumnInfo("catflags", classOf[java.lang.Long], "Flags")
    */

    val outputTable = new RowListStarTable(outputInfo)

    // val rec : Group = runQuery(center_ra, center_dec, cone_radius, conf.filename())
    val records  = runQuery(center_ra, center_dec, cone_radius, conf.filename())
    val parquet_fields = List(("matchid", "long"), 
                              ("ra", "double"),
                              ("dec", "double"))

    var object_row_n = 0
    for (rec <- records)  {
      val fixedCellList : List[AnyRef] = parquet_fields.map(
          _ match {
            case (columnName: String,  "long") => rec.getLong(columnName, 0) : java.lang.Long
            case (columnName: String,  "double") => rec.getDouble(columnName, 0) : java.lang.Double
          })

      // outputTable.addRow(List( rec.getLong("matchid", 0) : java.lang.Long, rec.getDouble("ra", 0) : java.lang.Double, rec.getDouble("dec", 0) : java.lang.Double).toArray[Object])
      outputTable.addRow(fixedCellList.toArray[Object].clone)
      object_row_n += 1
    }

    // outputTable.addRow(List(1234 : java.lang.Long, 32.2: java.lang.Double, 23.5: java.lang.Double).toArray[Object])

    val writer = new VOTableWriter()
    writer.writeStarTable(outputTable, System.out)

    /*
    val mjdGroup = rec.getGroup("mjd", 0)
    // println(rec.getType())

    val repCount = mjdGroup.getFieldRepetitionCount("list")
    println("rep: ", repCount)
    println("0: ", mjdGroup.getGroup("list", 0).getDouble("element", 0))
    println("0: ", mjdGroup.getGroup("list", 1).getDouble("element", 0))
    println("0: ", mjdGroup.getGroup("list", 2).getDouble("element", 0))
    println("0: ", mjdGroup.getGroup("list", repCount - 1).getDouble("element", 0))

    */
    val list_columns =  List(("mjd", "double"),
                             ("mag", "double"),
                             ("magerr", "double"),
                             ("filterid", "integer"),
                             ("catflags", "long"))

    
    /*


    val array_row_n = 0
    for (array_row_n <- 0 to repCount) {
      val outputRows = foreach s
      val 
      val arr_output_row = list_columns.map(
        _ match {
          case (columnName: String, "double") => rec.getGroup(columnName, 0).getGroup("list", output_row).getDouble("element", 0)
          case (columnName: String, "long") => rec.getGroup(columnName, 0).getGroup("list", output_row).getLong("element", 0)
          case (columnName: String, "integer") => rec.getGroup(columnName, 0).getGroup("list", output_row).getInteger("element", 0)
        })
    }
    */

  }

  /*
  def getArrayColumnElement(record, columnName, array_index, type) = {
    record.getGroup(columnName, 0).getGroup("list", output_row).getDouble("element", 0)
  }
  */

  def runQuery(center_ra : Double, center_dec : Double, cone_radius : Double, filename : String) : Iterator[Group] = {
    val zone = math.floor((center_dec + 90)/(1/60.0)).toLong

    val spatial = FilterApi.and(
                    FilterApi.and(FilterApi.gt(FilterApi.doubleColumn("ra"), (center_ra - cone_radius) : java.lang.Double),
                                  FilterApi.lt(FilterApi.doubleColumn("ra"), (center_ra + cone_radius): java.lang.Double)),
                    FilterApi.and(FilterApi.gt(FilterApi.doubleColumn("dec"), (center_dec - cone_radius): java.lang.Double),
                                  FilterApi.lt(FilterApi.doubleColumn("dec"), (center_dec + cone_radius): java.lang.Double)))
    var filter = FilterApi.and(spatial, FilterApi.eq(FilterApi.longColumn("zone"), zone: java.lang.Long))

                                               
    val builder = ParquetReader.builder(new GroupReadSupport(), new Path(filename))
      
    val reader = builder.withFilter(FilterCompat.get(filter)).build()

    Iterator.continually(reader.read).takeWhile(_ != null)
  }

}


