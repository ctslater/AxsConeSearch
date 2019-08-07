
package axs

import scala.math
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.example.data.Group
import org.apache.parquet.schema.MessageType
import org.apache.parquet.hadoop.{ParquetReader, ParquetFileReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate

import  uk.ac.starlink.table.{StarTable, ColumnInfo, RowListStarTable}
import uk.ac.starlink.votable.VOTableWriter

import scala.collection.JavaConversions._

/*
object ResultsLevel extends Enumeration {
  type ResultsLevel = Value
  val narrow, lightcurve, fulltable = Value
}
*/

sealed trait ResultsLevel
case object ResultsNarrow extends ResultsLevel
case object ResultsLightcurve extends ResultsLevel
case object ResultsFull extends ResultsLevel

class AxsConeSearch(val filename: String) {

  /** Return a VOTable containing objects near the specified point
   *  
   *  Coordinates and cone radius are in degrees.
   *
   */
  def search(center_ra : Double, center_dec : Double, cone_radius : Double, columns : ResultsLevel = ResultsLightcurve) = {

    val fixed_columns = List(("matchid", "long", Set(ResultsNarrow, ResultsLightcurve), "Object ID"), 
                              ("ra", "double", Set(ResultsNarrow, ResultsLightcurve), "RA"),
                              ("dec", "double", Set(ResultsNarrow, ResultsLightcurve), "Dec"))

    val list_columns =  List(("mjd", "double", Set(ResultsLightcurve), "MJD"),
                             ("mag", "float", Set(ResultsLightcurve), "Magnitude"),
                             ("magerr", "float", Set(ResultsLightcurve), "Mag Error"),
                             ("filterid", "long", Set(ResultsLightcurve), "Filter (1:g, 2:R, 3:i)"),
                             ("catflags", "integer", Set(ResultsLightcurve), "Flags"))


    val selected_fixed_columns = fixed_columns.filter( { case  (_, _, level, _) => level contains columns})
    val selected_list_columns = list_columns.filter( { case  (_, _, level, _) => level contains columns})

    val outputInfo : Array[ColumnInfo] = (selected_fixed_columns ++ selected_list_columns).map(_ match {
      case (colname, "long", _, doc) => new ColumnInfo(colname, classOf[java.lang.Long], doc)
      case (colname, "double", _, doc) => new ColumnInfo(colname, classOf[java.lang.Double], doc)
      case (colname, "float", _, doc) => new ColumnInfo(colname, classOf[java.lang.Float], doc)
      case (colname, "integer", _, doc) => new ColumnInfo(colname, classOf[java.lang.Integer], doc)
    }).toArray

    val outputTable = new RowListStarTable(outputInfo)

    val records  = AxsConeSearch.runQuery(center_ra, center_dec, cone_radius, filename, columns)

    for (rec <- records)  {
      val fixedCellList : List[AnyRef] = selected_fixed_columns.map(
          _ match {
            case (columnName: String, "long", _, _) => rec.getLong(columnName, 0) : java.lang.Long
            case (columnName: String, "double", _, _) => rec.getDouble(columnName, 0) : java.lang.Double
            case (columnName: String, "integer", _, _) => rec.getInteger(columnName, 0) : java.lang.Integer
            case (columnName: String, "float", _, _) => rec.getFloat(columnName, 0) : java.lang.Float
          })

      if(selected_list_columns.size == 0) {
        outputTable.addRow((fixedCellList).toArray[Object])
      } else {
        val array_col_len = rec.getGroup("mjd",0).getFieldRepetitionCount("list")
        for(array_col_n <- 0 to (array_col_len - 1)) {
          val arrayCellList : List[AnyRef] = selected_list_columns.map( _ match {
            case (columnName: String, "long", _, _) => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getLong("element", 0) : java.lang.Long
            case (columnName: String, "double", _, _) => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getDouble("element", 0) : java.lang.Double
            case (columnName: String, "integer", _, _) => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getInteger("element", 0) : java.lang.Integer
            case (columnName: String, "float", _, _) => rec.getGroup(columnName, 0).getGroup("list", array_col_n).getFloat("element", 0) : java.lang.Float
          })

          outputTable.addRow((fixedCellList ++ arrayCellList).toArray[Object])
        }
      }
    }

    val writer = new VOTableWriter()
    val outputStream = new java.io.ByteArrayOutputStream()
    writer.writeStarTable(outputTable, outputStream)
    outputStream
  }

}


object AxsConeSearch {


  def runQuery(center_ra : Double, center_dec : Double, cone_radius : Double, filename : String, columns : ResultsLevel) : Iterator[Group] = {
    val zone = math.floor((center_dec + 90)/(1/60.0)).toLong

    val spatial = FilterApi.and(
                    FilterApi.and(FilterApi.gt(FilterApi.doubleColumn("ra"), (center_ra - cone_radius) : java.lang.Double),
                                  FilterApi.lt(FilterApi.doubleColumn("ra"), (center_ra + cone_radius): java.lang.Double)),
                    FilterApi.and(FilterApi.gt(FilterApi.doubleColumn("dec"), (center_dec - cone_radius): java.lang.Double),
                                  FilterApi.lt(FilterApi.doubleColumn("dec"), (center_dec + cone_radius): java.lang.Double)))
    var filter = FilterApi.and(spatial, FilterApi.eq(FilterApi.longColumn("zone"), zone: java.lang.Long))

                                               
    val readSupport = new GroupReadSupport()

    val configuration = new Configuration()

    val readFooter = ParquetFileReader.readFooter(configuration, new Path(filename + "part-00000-721cf928-732a-4452-a1f9-37b4501c841a_00000.c000.snappy.parquet"))
    val schema = readFooter.getFileMetaData().getSchema()

    val okColumns = columns match {
      case ResultsNarrow => Set("matchid", "ra", "dec" )
      case ResultsLightcurve => Set("ra", "dec", "matchid", "mjd", "mag", "magerr", "filterid", "catflags")
      case ResultsFull => Set("ra", "dec", "matchid", "mjd", "mag", "magerr", "filterid", "catflags")
    }
    val subsetSchema = new MessageType(schema.getName(), schema.getFields().filter(x => okColumns contains x.getName()))

    /* println(subsetSchema) */
    readSupport.init(configuration, null, subsetSchema)
    val builder = ParquetReader.builder(readSupport, new Path(filename))
      
    val reader = builder.withFilter(FilterCompat.get(filter)).build()

    Iterator.continually(reader.read).takeWhile(_ != null)
  }

}

