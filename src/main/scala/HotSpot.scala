/*

This Scala program identify spatio-temporal hot spots in New York City Yellow Cab taxi trip using Getis-Ord Gi* statistic. 
Dataset records spanning  from January 2009 to June 2015. http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Major functions include:

1. point(longitude: String, latitude: String):  read and parse input data

2. parse(line: String) : parse input data to get (dropoff Time, dropoff Location, number of passengers)

3. spread_voxels(pvec: ((Int,Int,Long),Int), degree: Double, hour: Long) : spread values to its neighbors based on 
			Queen distance of 200m for the preceding, current, and following two hour time periods

4. zeroS(pvec: ((Int,Int,Long),Int)):get value*value and set key to 0 for S 

5. zeroX(pvec: ((Int,Int,Long),Int)):get value and set key to 0 for X 

6. normalCDF (x: Double) : calculate a standard normal CDF conversion w/ two tails based on G*

*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import java.io._
import org.apache.spark._
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.control._
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.stat.Statistics
import scala.math.{abs, atan, exp, floor, Pi, pow, sqrt}

object HotSpot {

	//set time format
  	val formatter = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm")

  	//Point: wrapper class contains longitude and latitude
  	case class Point(longitude: Double, latitude: Double)
  
  	//read and parse input data	
  	def point(longitude: String, latitude: String): Point = {
		new Point(longitude.toDouble, latitude.toDouble)
  	}

  	//parse input data to get (dropoff Time, dropoff Location, number of passengers)
  	def parse(line: String): (DateTime, Point, Int) = {
		val fields = line.split(',')
		val license = fields(0)
		val pickupTime = new DateTime(formatter.parse(fields(1)))
		val dropoffTime = new DateTime(formatter.parse(fields(2)))
		val passenger = fields(3).toInt
		val pickupLoc = point(fields(5), fields(6))
		val dropoffLoc = point(fields(9), fields(10))
		(dropoffTime, dropoffLoc, passenger)
  	}
 
	//spread values to its neighbors based on Queen distance of 200m for the preceding, current, and following two hour time periods
	def spread_voxels(pvec: ((Int,Int,Long),Int), degree: Double, hour: Long) = {
		val wind = -1 to 1
	  	val pos = pvec._1
	  	val scalevalue = pvec._2
	
		//spread values to the neighbors of current tube
	  	for(x <- wind; y <- wind; z <- wind)
	        	yield ((pos._1 + x, pos._2 + y,pos._3 + z), (pos._1, pos._2, pos._3, scalevalue))
	} 

	//get value*value and set key to 0 for S
	def zeroS(pvec: ((Int,Int,Long),Int)) = {
		val pos = pvec._1
		val scalevalue = pvec._2
		(0, scalevalue*scalevalue)
	}

	//get value and set key to 0 for X
	def zeroX(pvec: ((Int,Int,Long),Int)) = {
		val pos = pvec._1
		val scalevalue = pvec._2
		(0, scalevalue)
	}

	//calculate a standard normal CDF conversion w/ two tails based on G*
	def normalCDF (x: Double): Double = {
	    val a = Array (2.2352520354606839287,   1.6102823106855587881e2,
	                    1.0676894854603709582e3, 1.8154981253343561249e4,
	                    6.5682337918207449113e-2)

	    val b = Array (4.7202581904688241870e1, 9.7609855173777669322e2,
	                    1.0260932208618978205e4, 4.5507789335026729956e4)

	    val c = Array (3.9894151208813466764e-1, 8.8831497943883759412,
	                    9.3506656132177855979e1,  5.9727027639480026226e2,
	                    2.4945375852903726711e3,  6.8481904505362823326e3,
	                    1.1602651437647350124e4,  9.8427148383839780218e3,
	                    1.0765576773720192317e-8)

	    val d = Array (2.2266688044328115691e1, 2.3538790178262499861e2,
	                    1.5193775994075548050e3, 6.4855582982667607550e3,
	                    1.8615571640885098091e4, 3.4900952721145977266e4,
	                    3.8912003286093271411e4, 1.9685429676859990727e4)

	    val p = Array (2.1589853405795699e-1, 1.274011611602473639e-1,
	                    2.2235277870649807e-2, 1.421619193227893466e-3,
	                    2.9112874951168792e-5, 2.307344176494017303e-2)

	    val q = Array (1.28426009614491121,    4.68238212480865118e-1,
	                    6.59881378689285515e-2, 3.78239633202758244e-3,
	                    7.29751555083966205e-5)

	    val sixteen = 16.0
	    val M_1_SQRT_2PI = 0.398942280401432677939946059934  // 1/sqrt(2pi)
	    val M_SQRT_32 = 5.656854249492380195206754896838   // sqrt(32)    
	  
	    val eps = 2.2204460492503131e-016  // machine epsilon
	    val min = 2.2250738585072014e-308
          
	    var (ccum, cum) = (0.0, 0.0)
	    var (del, temp) = (0.0, 0.0)
	    var (xden, xnum, xsq) = (0.0, 0.0, 0.0)

	    var i = 0
	    val y = abs(x)

		//1st case: |x| <= qnorm(3/4)
		if (y <= 0.67448975) {
			if (y > eps) xsq = x * x
			xnum = a(4) * xsq
			xden = xsq
			for (i <- 0 until 3) {
				xnum = (xnum + a(i)) * xsq
				xden = (xden + b(i)) * xsq
			} // for
			temp = x * (xnum + a(3)) / (xden + b(3))
			cum  = 0.5 + temp
			ccum = 0.5 - temp

		//2nd case: qnorm(3/4)1 <= |x| <= sqrt(32)
		} else if (y <= M_SQRT_32) {
			xnum = c(8) * y
			xden = y
			for (i <- 0 until 7) {
				xnum = (xnum + c(i)) * y
				xden = (xden + d(i)) * y
			} // for
			temp = (xnum + c(7)) / (xden + d(7))
			xsq = floor (y*sixteen) / sixteen
			del = (y - xsq) * (y + xsq)
			cum = exp (-(xsq*xsq*0.5)) * exp(-(del*0.5)) * temp
			ccum = 1.0 - cum
			if (x > 0.0) { temp = cum; cum = ccum; ccum = temp }

		//3rd case: -37.5193 < x && x < 8.2924 || -8.2924  < x && x < 37.5193
		} else if (-37.5193 < x && x < 8.2924 || -8.2924 < x && x < 37.5193) {
			xsq = 1.0 / (x * x)
			xnum = p(5) * xsq
			xden = xsq
			for(i <- 0 until 4) {
				xnum = (xnum + p(i)) * xsq
			  	xden = (xden + q(i)) * xsq
			} 
			temp = xsq * (xnum + p(4)) / (xden + q(4))
			temp = (M_1_SQRT_2PI - temp) / y
			xsq = floor (x*sixteen) / sixteen
			del = (x - xsq) * (x + xsq)
			cum = exp (-(xsq*xsq*0.5)) * exp (-(del*0.5)) * temp
			ccum = 1.0 - cum
			if (x > 0.0) { temp = cum; cum  = ccum; ccum = cum }

		//4th case: high x values
		} else {
			if (x > 0) { cum = 1.0; ccum = 0.0 }
			else { cum = 0.0; ccum = 1.0 }
		} // if

		if (cum < min)  cum  = 0.0
		if (ccum < min) ccum = 0.0

		cum
	} //normalCDF

	def main(args: Array[String]) {

		/************************ create Spark context with Spark configuration ************************/

		val conf = new SparkConf().setAppName("Simple Application").set("spark.shuffle.blockTransferService", "nio")
		val sc = new SparkContext(conf)

		/************************************ Processing Input data ************************************/

		//get input data 
		val input = sc.textFile(args(0))	

		//dismiss header and null data to input data    
		val header = input.first()	
		val filterHeader = input.filter(x => x != header)	
		val filterNull = filterHeader.filter(x => x != "")	

		//parse input data to get drop off time, longitude and attitude of location, number of passangers
		val parseData = filterNull.map{ line => parse(line)}

		//filter data which 74.25 >= longitude >=73.7 and 40.5 >= latitude <= 40.9
		val filterData = parseData.filter(x => (abs(x._2.longitude) >= 73.7) && (abs(x._2.longitude) <= 74.25) && (abs(x._2.latitude) >= 40.5) && (abs(x._2.latitude) <= 40.9))   	  

		//read parameters	
		val hour: Long = args(3).toInt * 24 * 60 * 60  //Time step size --days*24*60*60 seconds 
		val degree = args(2).toDouble  //Cell size

		//set Cube parameters    
		//val lat_s = 0.0025  //200m latitude
		//val long_s = 0.0025 	//200m longitude	
		//val hour_s = 2 * 60 * 60  //2*60*60=7200 seconds

		//set time step origin
		val standrad1 = formatter.parse("2015/01/01 00:00")		
		val standrad = new DateTime(standrad1)

		//Index generation : (cell_x ID, cell_y ID, time_step ID, number of passangers )
    		val  IndexG = filterData.map(e => {
				val(x,y,z) = e

			 	//calculate Cube Index for each item
			 	var time_step = (x.getMillis() - standrad.getMillis()) / 1000 / hour
			 	var cell_x = (y.longitude / degree).toInt
			 	var cell_y = (y.latitude / degree).toInt
	 
			 	if(((x.getMillis()-standrad.getMillis()) / 1000) % hour != 0 ) {
					  if(time_step < 0) {
						time_step = time_step - 1
					  }
				 }
			 	if(y.longitude%degree != 0 ) {
				  	if(cell_x < 0) {
						cell_x = cell_x - 1
					}
			 	}
			 	if(y.latitude%degree != 0 ) {
				  	if(cell_y < 0) {
						cell_y = cell_y-1
				  	}
			 	}
	  			((cell_x, cell_y, time_step), z)
     		})

		//combine people number in the same Cube  
		val tubeCombine = IndexG.map(e => (e._1,e._2)).reduceByKey((a, b) => a + b) //combine values with the same key
		
		//store tubeCombine to main memory for using again
		tubeCombine.cache() 

		/******************************* calculate G* and Pvalue **********************************/

		//total number of cells	
		val cellN = tubeCombine.count()   

		//calcualte Xavg value		
		val X = tubeCombine.map(cvec => zeroX(cvec)).reduceByKey((a, b) => a + b).first()._2   
		val Xavg = X.toDouble / cellN 

		//calculate Savg value    	
		val S = tubeCombine.map(cvec => zeroS(cvec)).reduceByKey((a, b) => a+b).first()._2  
		val Savg = math.sqrt(S.toDouble/cellN - Xavg * Xavg)
		
		//spread values to its neighbors based on Queen distance of 200m for the preceding, current, and following two hour time periods
		val spreadValue = tubeCombine.flatMap(cvec => spread_voxels(cvec,degree,hour)).groupByKey()

		//calculate Gstar and Pvalue 	
		val Gstar = spreadValue.map(e => {
			//k: current tube index, v: array buffer includes (tube index, value) of its neighbors 
			val(k,v) = e
			var select = 0	
			var length = v.size()
			var accum : Double = 0
			var gstar : Double = (-1000000)
			val loop = new Breaks
			var pValue : Double = (-1000000)
			var own = 0

			//check current tube available or not
			//check array buffer whether includes current tube index. if true, select == 1 else select == 0
			loop.breakable {
				v.foreach(a => {
					if(a._1 == k._1 && a._2 == k._2 && a._3 == k._3) {
						select = 1
						own = a._4
						loop.break
					}
				})
			}
			// for available tube, calculate G* and Pvalue  			
			if(select == 1) {
				//do accumulation(Wi,j * Xj) which Wi,j of neighbor is 1, otherwise 0 
				v.foreach(a => {
						accum = accum+ a._4.toDouble
				})

				//accumulation decrease itself
				accum = accum - own
				length = length - 1

				//process tubes without neighbors
				if(length == 0) {
					 gstar = (-1000000)
				} else {
					//calculate G*
					gstar = (accum - length * (Xavg.toDouble)) / (Savg * math.sqrt(((cellN * length - length * length).toDouble / (cellN - 1))))
				}
			}

			 //calculate pValue : a standard normal CDF conversion w / two tails based on G*
			 pValue = 2 * (1 - normalCDF(math.abs(gstar)))

			 //(cell_x, cell_y, time_step, zscore, pvalue)
			 (k._1, k._2, k._3, gstar, pValue)
		})

		//filter and sort result     	
		val Gresult = Gstar.filter(e => e._4 != -1000000.toDouble )
		val Gresult1 = Gresult.sortBy(_._4, false)  	
		
		//save result
		Gresult1.saveAsTextFile(args(1))

	} //main

} //HotSpot
