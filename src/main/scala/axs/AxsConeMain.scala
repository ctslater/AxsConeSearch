
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
    val outputInfo : Array[ColumnInfo] = Array.ofDim(8)
    outputInfo(0) = new ColumnInfo("matchid", classOf[java.lang.Long], "Object ID")
    outputInfo(1) = new ColumnInfo("ra", classOf[java.lang.Double], "RA")
    outputInfo(2) = new ColumnInfo("dec", classOf[java.lang.Double], "Dec")
    outputInfo(3) = new ColumnInfo("mjd", classOf[java.lang.Double], "MJD")
    outputInfo(4) = new ColumnInfo("mag", classOf[java.lang.Float], "Mag")
    outputInfo(5) = new ColumnInfo("magerr", classOf[java.lang.Float], "Mag Error")
    outputInfo(6) = new ColumnInfo("filterid", classOf[java.lang.Long], "Filter (1:g, 2:R, 3:i)")
    outputInfo(7) = new ColumnInfo("catflags", classOf[java.lang.Integer], "Flags")

    val outputTable = new RowListStarTable(outputInfo)

    // val rec : Group = runQuery(center_ra, center_dec, cone_radius, conf.filename())
    val records  = runQuery(center_ra, center_dec, cone_radius, conf.filename())
    val parquet_fields = List(("matchid", "long"), 
                              ("ra", "double"),
                              ("dec", "double"))

    val list_columns =  List(("mjd", "double"),
                             ("mag", "float"),
                             ("magerr", "float"),
                             ("filterid", "long"),
                             ("catflags", "integer"))

    for (rec <- records)  {
      val fixedCellList : List[AnyRef] = parquet_fields.map(
          _ match {
            case (columnName: String,  "long") => rec.getLong(columnName, 0) : java.lang.Long
            case (columnName: String,  "double") => rec.getDouble(columnName, 0) : java.lang.Double
            case (columnName: String,  "integer") => rec.getInteger(columnName, 0) : java.lang.Integer
            case (columnName: String,  "float") => rec.getFloat(columnName, 0) : java.lang.Float
          })

      // outputTable.addRow(List( rec.getLong("matchid", 0) : java.lang.Long, rec.getDouble("ra", 0) : java.lang.Double, rec.getDouble("dec", 0) : java.lang.Double).toArray[Object])
      val array_col_len = rec.getGroup("mjd",0).getFieldRepetitionCount("list")
      for(array_col_n <- 0 to (array_col_len - 1)) {
        val arrayCellList : List[AnyRef] = list_columns.map( _ match {
          case (columnName: String, "long") => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getLong("element", 0) : java.lang.Long
          case (columnName: String, "double") => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getDouble("element", 0) : java.lang.Double
          case (columnName: String, "integer") => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getInteger("element", 0) : java.lang.Integer
          case (columnName: String, "float") => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getFloat("element", 0) : java.lang.Float
        })

        outputTable.addRow((fixedCellList ++ arrayCellList).toArray[Object])
      }
    }


    val writer = new VOTableWriter()
    writer.writeStarTable(outputTable, System.out)


  }

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


