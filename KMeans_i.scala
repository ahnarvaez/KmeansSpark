import org.antlr.v4.runtime.atn.SemanticContext.AND
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.DenseVector

import scala.runtime.ScalaRunTime._
import scala.util.Random
import scala.util._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.parquet.filter2.predicate.Operators.And
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks
import scala.util.control.Breaks._

class KMeasn_i(var X: Array[Array[Double]],var k: Int =3, var init: String="Random") extends Serializable {
  val random = new java.security.SecureRandom
  var centroides: List[Array[Double]] = null
  var antiguos_centroides: List[Array[Double]] = null
  var sTiempos:String=""

  def fit(): Unit ={
    //iniciando sesion en Spark
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("kmeans_spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //Paralelizando la base de datos
    val rddPuntos=sc.parallelize(X)
    var tini:Double=0
    //sacando los centroides originales para la opcion Random
    if (init=="Random") {
      centroides = rddPuntos.takeSample(false, k).collect{case x:Array[Double] => x}.toList
    }
    //Obteniendo los centorides para la opcion Kmeans++
    else if(init=="Kmeans++")
    {
      val t_0 = System.nanoTime()
      centroides=kmeanspp(rddPuntos)
      val t_1 = System.nanoTime()
      println("Tiempo de inicializacion kmeans ++:" +(t_1-t_0)+" , "+(t_1-t_0).toDouble/1E9+"s")
      tini=(t_1-t_0).toDouble/1E9
    }
    println("Centroides originales")
    println(stringOf(centroides))


    var iteraciones:Int=0
    val t0 = System.nanoTime()
    do
    {
      iteraciones+=1

      //Genernado nuevo RDD a partir de las distancias de los elementros a los centroides
      var rdd = rddPuntos.map(distancias)


      //Agrupando elementos mediante el id del centroide
      var rdd_agrupado=rdd.groupByKey()

      //guardando centroides anteriores para la comparaciòn
      antiguos_centroides=centroides

      //calculando nuevos centroides
      centroides=rdd_agrupado.map(c_nuevos_centroides).collect().toList


    }while(!c_iguales(centroides,antiguos_centroides))

    val t1 = System.nanoTime()
    val seconds: Double =(t1-t0).toDouble/1E9
    println("Elapsed time: " + (t1-t0) + "ns |" + seconds + " s")
    println("nuevos"+stringOf(centroides))
    println("interaciones: "+stringOf(iteraciones))
    println("---------------------")
    sc.stop()
    //val x_3= x_2.filter{case(key, _) => key == "token"}.values.flatMap(i => i.toList).collect()
    //println("array", stringOf(rddPuntos.collect()))
    val tkmeans:String=stringOf((t1-t0).toDouble/1E9)+" s"
    val t_total:Double=tini+(t1-t0).toDouble/1E9
    sTiempos=stringOf(t_total) + ","+ stringOf(tini)+","+stringOf((t1-t0).toDouble/1E9)

  }

  def getStiempos():String = {
    return sTiempos
  }

  def getCentroides() : List[Array[Double]] =
  {
    return centroides
  }

  def distancias(x: Array[Double]): Tuple2[Int,Array[Double]] =
  {
    var dist: Double=0
    var llave:Int = 0
    for (e <- 0 to (centroides.length-1))
    {
      if(e==0 || dist > Vectors.sqdist(Vectors.dense(x), Vectors.dense(centroides(e))))
      {
        dist = Vectors.sqdist(Vectors.dense(x), Vectors.dense(centroides(e)))
        llave= e
      }
    }
    var t= new Tuple2(llave, x)
    return t
  }
  def c_nuevos_centroides (x : Tuple2[Int,  Iterable[Array[Double]]]) : Array[Double] =
  {
    var tempArray: Array[Double] = null
    var count:Int = 0
    var l: Int=0

    for(s <- x._2)
    {
      if(count==0)
        tempArray=Array.ofDim(s.length)
      count+=1
      for (i<-0 to (s.length-1))
      {
        tempArray(i)+=s(i)
      }
      l = s.length
    }
    for (i<-0 to (l-1))
    {
      tempArray(i)=tempArray(i)/count
    }
    //print(stringOf(tempArray))
    return tempArray
  }

  def imprimir (x : Tuple2[Int,  Array[Double]]) : Unit =
  {
    println(stringOf(x._1)+":"+stringOf(x._2))
  }

  def c_iguales(x:List[Array[Double]],y:List[Array[Double]]) : Boolean=
  {
    //val t0 = System.nanoTime()

    var resp: Boolean= false
    /*
    println(stringOf(x))
    println(stringOf(y))
    println(stringOf(x.length))
    println(stringOf(y.length))
    println(stringOf(x(0).length))
    */
    if(x.length != k || y.length != k)
    {
      resp=false

    }
    else
    {
      val outer = new Breaks;
      val inner = new Breaks;
      var tbool: Boolean=false
      outer.breakable {
        for (i <- 0 to (x.length - 1)) {
          tbool=false
          inner.breakable {
            for (j <- 0 to (y.length - 1)) {
              //println(stringOf(x(i))+":"+stringOf(y(j)))
              if (x(i).sameElements(y(j))) {
                //println("Iguales")
                tbool = true
                inner.break()
              }
            }
          }
          if(tbool==false) resp=false
          else resp=true
          if (resp == false) outer.break()
        }
      }
    }
    //val t1 = System.nanoTime()
    //val seconds: Double =(t1-t0).toDouble/1E9
    //println("Elapsed time: " + (t1-t0) + "ns |" + seconds + " s")

    return resp

  }
  def kmeanspp(datosRDD: RDD[Array[Double]]):List[Array[Double]] =
  {
    var s: Array[Array[Double]]=null
    s =datosRDD.takeSample(false,1)
    for(i <- 1 to (k-1))
    {
      //Distancias cuadraticas menores a los centroides
      var distancias_ = datosRDD.map(x => distm_kmpp(x,s))

      //Sumatoria de las sustancias menores a los centroides
      var total = distancias_.sum()

      //Porcion de la probabilidad que le corresponde a cada miembro.
      var porcentajes = distancias_.map(x => x/total).collect()

      //Sumatoria de los miembros con los valores anteriores para definir su posicion de 0 a 1.
      //luego de esto se verificara que el valor sea menor en cada miembro asi sabremos en que lugar se encuentra

      porcentajes=porcentajes.map{var s: Double = 0; d => {s += d; s}}
      //println("-j-"+stringOf(porcentajes))

      //numero aleatorio entre 0 y 1
      var rv=random.nextDouble()

      //println("Random value: "+stringOf(rv)+"  k:"+stringOf(k))

      //Ciclo detenible
      val outer = new Breaks;
      outer.breakable{
        for (i <- 0 to (porcentajes.length-1))
        {
          if(rv<porcentajes(i))
          {
            //Aregando nuevo centroide
            //println("Nvo Cent: "+stringOf(X(i))+" i:"+i)
            s=s++Array(X(i))
            outer.break()
          }
        }
      }
    }
    //println("SSSS"+stringOf(s))
    return s.toList
  }
  def distm_kmpp(vector: Array[Double],n_centroides:Array[Array[Double]]): Double =
  {
    var min: Double=0
    var pib: Double=0
    for (i <-0 to (n_centroides.length-1))
    {
      if (i==0) min = Vectors.sqdist(Vectors.dense(vector), Vectors.dense(n_centroides(i)))
      else
      {
        pib= Vectors.sqdist(Vectors.dense(vector), Vectors.dense(n_centroides(i)))
        if(min > pib)
          min=pib
      }
    }
    return min
  }

}