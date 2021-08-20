import java.time._
import java.nio.file.{Path, Paths, Files}

object Bls extends App {
  // from BLS documentation https://download.bls.gov/pub/time.series/jt/jt.txt
  val series = """
survey abbreviation    =        JT
seasonal (code)        =        S
industry_code        =        000000
state_code        =        00
area_code        =        00000
sizeclass_code        =        00
dataelement_code    =        JO
ratelevel_code        =        R"""

  /*
    We want to use these column names and need the field lenght to do so.
    To that end, we will split the metadata by line
    where each line is turned into a tuple with the column name and byte count
   */

  var idx: Int = 0; // Ooops. A variable in Scala? Heresy!

  val seriesMeta = series
    .split("\n")
    .map(s => {
      val sv = s.split("=")
      if (sv.size == 2) {
        idx += sv(1).trim.size //Position at which to insert a delimiter
        val ret = (sv(0).trim, idx)
        idx += 1;
        ret // Wanna make sure the increment happens after return value is calculated
      }
    })
    .collect { case (k, v) => (k.toString, v.toString.toInt) }
    .toVector // filter(_.isInstanceOf[Tuple2[String, Int]]) // In rust there is a filter_map, which filters out anything that's not some.

  //seriesMeta.foreach(println)

  val seriesHeader = seriesMeta
    .map(_._1.replace(" ", "_").replace("(", "").replace(")", ""))
    .mkString("\t")

  //println(seriesHeader)

  def tabulate(ins: String, idxs: Vector[Int]): String = {
    idxs.foldLeft(ins)(_.patch(_, "\t", 0))
  }

  //println(tabulate("JTS000000000000000JOR", seriesMeta.map(_._2)))

  def getData(url: String) = {

    val text = scala.io.Source.fromURL(url).mkString
    val newline = text.indexOf("\n")

    Map(
      "header" -> text.substring(0, newline - 1),
      "rows" -> text.substring(newline + 1, text.size)
    )
  }

  var startTime = Instant.now()
    val formatter = java.text.NumberFormat.getIntegerInstance

  val data = getData(
    "https://download.bls.gov/pub/time.series/jt/jt.data.2.JobOpenings"
  )

  println("Size of data: " + data.size)

  println(s"Fetched data in ${formatter.format(Duration.between(startTime, Instant.now).toMillis)} milliseconds")  

  val fileRaw = Paths.get("/home/lars/data/jolts.scala.raw")
  startTime = Instant.now()
  Files.writeString(fileRaw, data("rows"));
  println(s"Wrote file in ${formatter.format(Duration.between(startTime, Instant.now).toMillis)} milliseconds")  

 

  println(data("header"))

  startTime = Instant.now()
  val expanded = data("rows")
    .split("\n").par
    .map(row => {
      if (row.size > 52) {
        tabulate(row.substring(0, 21), seriesMeta.map(_._2)) + row.substring(21, row.size) + "\n"
      }
    })
    .reduce(_ + _.toString()).toString

   println(s"Expanded data in ${formatter.format(Duration.between(startTime, Instant.now).toMillis)} milliseconds")  

    val filePath = Paths.get("/home/lars/data/jolts.scala.tsv")

  startTime = Instant.now()
  Files.writeString(filePath, expanded);
  println(s"Wrote file in ${formatter.format(Duration.between(startTime, Instant.now).toMillis)} milliseconds")  


}
