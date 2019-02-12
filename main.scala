import scala.io.Source
import java.io._
import scala.runtime.ScalaRunTime._
//import co.theasi.plotly



object main {
  val random = new java.security.SecureRandom
  var centroides: List[Array[Double]] = null
  def main(args: Array[String]) {

    var arreglo:Array[Array[Double]]= readCSV()
    //println(stringOf(datos(0)))

    println(stringOf(arreglo(0)))
    //arreglo.transform(_.drop(1))
    println(stringOf(arreglo(0)))


    //val arreglo=random2dArray(100000,2)
    var ciclos:Int=10

    var valor_k: Int=20

    var x = new KMeasn_i(arreglo, k = valor_k, "Kmeans++")
    x.fit()
    var x_centroides: Array[Array[Double]] = x.getCentroides().toArray
    println(stringOf(x_centroides))


    for(i <- 0 to (ciclos-1))
    {
    var path="/home/misero/machine/kmpp_spark_"+i+".csv"
    var pw = new PrintWriter(new File( path))
    var tiempos = new PrintWriter(new File( "/home/misero/machine/kmpp_spark_tiempos.csv"))
    //pw.write("Valor k,Tiempo total,tiempo inicio,tiempo algoritmo\r\n")

    var x = new KMeasn_i(arreglo, k = valor_k, "Kmeans++")
    x.fit()

    var x_centroides: Array[Array[Double]] = x.getCentroides().toArray
      for (j<-x_centroides)
      {
        //arr+=stringOf(j(0))+":"+stringOf(j(1))+","
        pw.write(stringOf(j)+"\r\n")
        tiempos.write(stringOf(x.getStiempos())+"\r\n")
      }
      //pw.write(stringOf(valor_k)+","+x.getStiempos()+","+arr+"\r\n")
      pw.close()
      tiempos.close()
    }
/*
    for(i <- 0 to (ciclos-1))
    {
      var path="/home/misero/machine/kmRandom_spark_"+i+".csv"
      var pw = new PrintWriter(new File( path))
      var tiempos = new PrintWriter(new File( "/home/misero/machine/kmRandom_spark_tiempos.csv"))
      //pw.write("Valor k,Tiempo total,tiempo inicio,tiempo algoritmo\r\n")

      var x = new KMeasn_i(arreglo, k = valor_k, "Kmeans++")
      x.fit()

      var x_centroides: Array[Array[Double]] = x.getCentroides().toArray
      for (j<-x_centroides)
      {
        //arr+=stringOf(j(0))+":"+stringOf(j(1))+","
        pw.write(stringOf(j)+"\r\n")
        tiempos.write(stringOf(x.getStiempos())+"\r\n")
      }
      //pw.write(stringOf(valor_k)+","+x.getStiempos()+","+arr+"\r\n")
      pw.close()
      tiempos.close()
    }
*/
  }
  def random2dArray(dim1: Int, dim2: Int) = Array.fill(dim1, dim2) { random.nextDouble()*20 }

  def readCSV() : Array[Array[Double]] = {
    //Source.fromFile("/home/misero/machine/datakm1.csv")
    //Source.fromFile("/home/misero/machine/3D_spatial_network.txt")
    Source.fromFile("/home/misero/machine/eb.csv")
      .getLines()
      .map(_.split(",").map(_.trim.toDouble))
      .toArray
  }
}
