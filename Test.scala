/**
 * Title:       Test.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Modified on: 3/4/2015
 * Description: Given a stream of N data points: (score, category), find the top k points with the highest scores, and
 *              calculate the rank and statistical significance of categories of the top k points. Stream is the file
 *              input with score and category separated by a tab, a binary max heap stores the top k data points and
 *              stores the information pertaining to population size, success states in population, number of draws,
 *              and the number of successes in the top k data points. Once those values have been gathered, the hyper-
 *              geometric distribution is calculated for each category and merge sort is used to sort the categories
 *              based on the hyper-geometric distribution from highest to lowest.
 * Build with:  Scala IDE (Eclipse or IntelliJ) or using the following commands on the glab machines
 *              To compile: scalac *.scala 
 *              To run:     scala CalculateScores input.txt 
 * Notes:       Concurrent Version
 * Credit:      We used this merge sort: http://eddmann.com/posts/merge-sort-in-scala-using-tail-recursion-and-streams/
 *              as inspiration for our merge sort, which we heavily edited and made concurrent. All other code was made
 *              entirely and only by Tony Zheng and Andrew Baumann. 
 */

import scala.math.BigDecimal
import scala.actors.Actor
import scala.actors.Actor._
import scala.io.Source
import scala.collection.mutable._

case object ReaderMessage1
case object ReaderMessage2
case object ReaderMessage3
case object ReaderMessage4
case object UniqueEndMessage

class Test(aFile:String) {
  private type T2 = Tuple2[Double, String]
  private var myPQ1, myPQ2, myPQ3, myPQ4, myPQF = PriorityQueue[T2]()(Ordering.by(doThis))
  private var numK, populationSize, draws:Int = 0
  private var myCategoryList, myMod1CategoryList, myMod2CategoryList:List[Category] = List()
  private var myMod3CategoryList, myMod4CategoryList:List[Category] = List()
  private var myStringList1, myStringList2, myStringList3, myStringList4:List[String] = List()
  private val fileName = aFile

  /**
   * Method that splits a string seperated by a tab, converts the first part to a double and returns it
   */
  def doThis(x:T2):Double = {
    return x._1
  }

  /**
   * Method that splits a string seperated by a tab, and returns the second part of the string
   */
  def uniqueCategories(x:T2):String = {
    return x._2
  }

  /**
   * Method that asks the user what the value of k is (tail recursive)
   */
  def getK():Unit = {
    print("Enter value of K: ")

    try {
      numK = Console.readInt()
      draws = numK
    }
    catch {
      case e: Exception => println("Exception caught: " + e + "\n")
        getK()
    }
  }

  /**
   * Method that has actors parse the file and inserts them into a heap of size k respectively
   */
  def run():Unit = {
    println("Opening: " + fileName + "\n")
  	var count:Int = 0
  	val firstActor, secondActor, thirdActor, fourthActor:Readers = new Readers

  	firstActor.start
  	secondActor.start
    thirdActor.start
    fourthActor.start
  	firstActor ! ReaderMessage1
  	secondActor ! ReaderMessage2
    thirdActor ! ReaderMessage3
    fourthActor ! ReaderMessage4

    receive {
  		case UniqueEndMessage => count += 1
  	}
  	receive {
  		case UniqueEndMessage => count += 1
  	}
    receive{
      case UniqueEndMessage => count += 1
    }
    receive{
      case UniqueEndMessage => count += 1
    }
    combineQueues()
  }

  /**
   * Readers class that extends actors to read the file
   */
  class Readers extends Actor {
		def act {
  		while (true) {
			  receive {
				  case ReaderMessage1 =>
				    runMod0()
				    reply {
              UniqueEndMessage
            }
				    exit()
				  case ReaderMessage2 =>
            runMod1()
				    reply {
              UniqueEndMessage
            }
            exit()
          case ReaderMessage3 =>
            runMod2
            reply{
              UniqueEndMessage
            }
            exit()
          case ReaderMessage4 =>
            runMod3
            reply{
             UniqueEndMessage
            }
            exit()
        }
      }
    }
  }
  
  /**
   * Method that streams the file input and creates the binary max heap of the data points based on their values
   * private var myCategoryList, myMod1CategoryList, myMod2CategoryList:List[Category] = List()
   * private var MyMod3CategoryList, MyMod2CategoryList:List[Category] = List()
   */
  def runMod0():Unit = {
    var temp, counter:Int = 0;
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
      if(populationSize % 4 == 0) {
  	 	  myPQ1.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))

        if(temp <= numK) {
          checkIfUnique1((line.split('\t')(0).toDouble, line.split('\t')(1)))
        	temp += 1
    		}
      	else {
          minimum = myPQ1.min
          checkIfUnique1((line.split('\t')(0).toDouble, line.split('\t')(1)))
        	myPQ1 = myPQ1.filterNot(it => it == minimum)
     		}
     	}
      populationSize += 1
      counter += 1
    }
  }

  def runMod1():Unit = {

    var count, temp:Int = 0
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
      if (count % 4 == 1) {
        myPQ2.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))
    
        if(temp <= numK) {
          checkIfUnique2((line.split('\t')(0).toDouble, line.split('\t')(1)))
          temp += 1
        }
        else {
          minimum = myPQ2.min
          checkIfUnique2((line.split('\t')(0).toDouble, line.split('\t')(1)))
          myPQ2 = myPQ2.filterNot(it => it == minimum)
        }
      }
      count += 1
    }
  }

  def runMod2():Unit = {
    var count, temp:Int = 0
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
      if (count % 4 == 2) {
        myPQ3.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))

        if(temp <= numK) {
          checkIfUnique3((line.split('\t')(0).toDouble, line.split('\t')(1)))
          temp += 1
        }
        else {
          minimum = myPQ3.min
          checkIfUnique3((line.split('\t')(0).toDouble, line.split('\t')(1)))
          myPQ3 = myPQ3.filterNot(it => it == minimum)
        }
      }
      count += 1
    }
  }

  def runMod3():Unit = {
    var count, temp:Int = 0
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
      if(count % 4 == 3) {
        myPQ4.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))

        if(temp <= numK) {
          checkifUnique4((line.split('\t')(0).toDouble, line.split('\t')(1)))
          temp += 1
        }
        else {
          minimum = myPQ4.min
          checkifUnique4((line.split('\t')(0).toDouble, line.split('\t')(1)))
          myPQ4 = myPQ4.filterNot(it => it == minimum)
        }
      }
      count += 1
    }
  }

  def combineQueues():Unit = {
    var count:Int = numK
    var temp:Int = 0

    combineLists()

    while(myPQ1.size != 0) {
      var temp1 = myPQ1.dequeue
      myPQF += temp1
    }
    while(myPQ2.size != 0) {
      var temp2 = myPQ2.dequeue
      myPQF += temp2
    }
    while(myPQ3.size != 0) {
      var temp3 = myPQ3.dequeue
      myPQF += temp3
    }
    while(myPQ4.size != 0) {
      var temp4 = myPQ4.dequeue
      myPQF += temp4
    }
    while(myPQF.size > numK) {
      myPQF = myPQF.filterNot(it => it == myPQF.min)
    }
  }

  /**
   * Method that checks if the file input belongs to a category and increments the successes if it does, else creates a
   * new category and places it into a list
   * private var myCategoryList:List[Category] = List()
   * private var myMod1CategoryList:List[Category] = List()
   */

  def checkIfUnique1(x:T2):Unit = {
    val categoryName = x._2

    if(!myStringList1.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringList1 = myStringList1 :+ categoryName
      myMod1CategoryList = myMod1CategoryList :+ myCategory
    }
    else {
      myMod1CategoryList(myStringList1.indexOf(categoryName)).incrementCount()
    }
  }

  def checkIfUnique2(x:T2):Unit = {
    val categoryName:String = x._2

    if(!myStringList2.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringList2 = myStringList2 :+ categoryName
      myCategoryList = myCategoryList :+ myCategory
    }
    else {
      myCategoryList(myStringList2.indexOf(categoryName)).incrementCount()
    }
  }

  def checkIfUnique3(x:T2):Unit = {
    val categoryName = x._2

    if(!myStringList3.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringList3 = myStringList3 :+ categoryName
      myMod3CategoryList = myMod3CategoryList :+ myCategory
    }
    else {
      myMod3CategoryList(myStringList3.indexOf(categoryName)).incrementCount()
    }
  }

  def checkifUnique4(x:T2):Unit = {
    val categoryName = x._2

    if(!myStringList4.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringList4 = myStringList4 :+ categoryName
      myMod4CategoryList = myMod4CategoryList :+ myCategory
    }
    else {
      myMod4CategoryList(myStringList4.indexOf(categoryName)).incrementCount()
    }
  }

  /**
   * method that combines all of the lists
   */
  def combineLists():Unit = {
  	var countOdd, countEven:Int = 0

  	while(countOdd < myMod1CategoryList.size) {
  		var tempname = myMod1CategoryList(countOdd).getName
  		var tempcount = myMod1CategoryList(countOdd).getCount

      if(!myStringList2.contains(tempname)) {
	      val myCategory = new Category()
	      myCategory.setName(tempname)
	      myCategory.setCount(tempcount)
	      myCategoryList = myCategoryList :+ myCategory  	
  		}
  		else {
  		  while(countEven < myCategoryList.size) {
  		  	myCategoryList(countEven).compare(myMod1CategoryList(countOdd))
  		  	countEven += 1
  		  }
      }
  		countEven = 0
  		countOdd += 1
  	}

    countOdd=0
    countEven=0

    while(countOdd < myMod3CategoryList.size) {
      var tempname = myMod3CategoryList(countOdd).getName
      var tempcount = myMod3CategoryList(countOdd).getCount

      if(!myStringList3.contains(tempname)) {
        val myCategory = new Category()
        myCategory.setName(tempname)
        myCategory.setCount(tempcount)
        myCategoryList = myCategoryList :+ myCategory   
      }
      else {
        while(countEven < myCategoryList.size) {
          myCategoryList(countEven).compare(myMod3CategoryList(countOdd))
          countEven += 1
        }
      }
      countEven = 0
      countOdd += 1
    }

    countOdd=0
    countEven=0

    while(countOdd < myMod4CategoryList.size) {
      var tempname = myMod4CategoryList(countOdd).getName
      var tempcount = myMod4CategoryList(countOdd).getCount

      if(!myStringList4.contains(tempname)) {
        val myCategory = new Category()
        myCategory.setName(tempname)
        myCategory.setCount(tempcount)
        myCategoryList = myCategoryList :+ myCategory   
      }
      else {
        while(countEven < myCategoryList.size) {
          myCategoryList(countEven).compare(myMod4CategoryList(countOdd))
          countEven += 1
        }
      }
      countEven = 0 
      countOdd += 1
    }
  }

  /**
   * Method that prints all the values in the binary max heap (tail recursive)
   */
  def printAll():Unit = {
    printPQ(myPQF)

    def printPQ(it:PriorityQueue[T2]):Unit = {
      if (it.isEmpty) {
        return
      }
      println(it.head._1)
      printPQ(it.tail)
    }
    println()
  }

  def getTopK():Unit = {
    println("The top k datapoints are: ")
    printPQ(myPQF)

    def printPQ(it:PriorityQueue[T2]):Unit = {
      if (it.isEmpty) {
        return
      }
      println(it.head._2 + " - " + it.head._1)
      printPQ(it.tail)
    }
    println()
  }

  /**
   * Method that prints all of the success states there are and their success states in the population
   */
  def printCategories():Unit = {
    for(x <- myCategoryList){
      println(x.getName() + ": " + x.getCount())
    }
    println()
  }

  /**
   * Method that gets the number of successes in the top k data points that belong to a category (tail recursive)
   */
  def getSuccesses(x:Category):Int = {
    def traversePQ(it:PriorityQueue[T2], counter:Int):Int = {
      if(it.isEmpty) {
        return counter
      }

      if(uniqueCategories(it.head) == x.getName()) {
        traversePQ(it.tail, counter+1)
      }
      else {
        traversePQ(it.tail, counter)
      }
    }
    return traversePQ(myPQF, 0)
  }

  /**
   * Method that prints out the statistical significance of each of the categories there are
   */
  def getStats():Unit = {
    for(x <- myCategoryList) {
      //println("FOR: " + x.getName())
      //println("Population size is: " + populationSize)
      //println("Total success states in population is: " + x.getCount())
      //println("Number of draws is: " + draws)
      //println("Number of draws in top " + numK + " is: " + getSuccesses(x))
      x.setHypergeometricDistribution(hypergeometricDistribution(populationSize, x.getCount(), draws, getSuccesses(x)))
      //println("Hypergeometric distribution is: " + x.getHypergeometricDistribution())
      //println()
    }
  }

  /**
   * Method that calculates the hyper-geometric distribution with nested methods that calculates the binomial
   * coefficient and factorial
   */
  def hypergeometricDistribution(N:Int, K:Int, n:Int, k:Int):BigDecimal = {
    def factorial(n:BigDecimal,result:BigDecimal):BigDecimal = {
      if(n <= 1) {
        return result
      }
      else {
        factorial(n - 1, n * result)
      }
    }

    def binomialCoefficient(a:BigDecimal, b:BigDecimal):BigDecimal = 
    {
      var bigdone, bigdtwo, bigdthree:BigDecimal = 0
      class biDoer extends Actor {
        def act{
          receive {
            case ReaderMessage2 => 
              bigdone = factorial(a,1)
              reply {
                UniqueEndMessage
              }
              exit()
            case ReaderMessage1 =>
              bigdtwo = factorial(b,1)
              reply {
                UniqueEndMessage
              }
              exit()
            case ReaderMessage3 =>
              bigdthree = factorial((a-b),1)
              reply {
                UniqueEndMessage
              }
              exit()
          }
        }
      }
      val firstActor,secondActor,thirdActor:biDoer = new biDoer

      firstActor.start
      secondActor.start
      thirdActor.start
      firstActor ! ReaderMessage2
      secondActor ! ReaderMessage1
      thirdActor ! ReaderMessage3

      receive {
        case UniqueEndMessage =>
      }
      receive {
        case UniqueEndMessage =>
      }
      receive {
        case UniqueEndMessage =>
      }
      return (bigdone/(bigdtwo * bigdthree))
    //  return (factorial(a, 1) / (factorial(b, 1) * factorial((a - b),1)))
    } /////////
    //return ((binomialCoefficient(K, k) * binomialCoefficient((N - K), (n - k))) / binomialCoefficient(N, n))
    var KOverk:BigDecimal = 0
    var bigOverLittle:BigDecimal = 0
    var Novern:BigDecimal = 0

    class Doer extends Actor
    {
      def act{
        receive 
        {
          case ReaderMessage2 =>
            KOverk = binomialCoefficient(K,k)
            reply {
              UniqueEndMessage
            }
            exit()
          case ReaderMessage1 =>
            bigOverLittle = binomialCoefficient((N-K), (n-k))
            reply {
              UniqueEndMessage
            }
            exit()
          case ReaderMessage3 =>
            Novern = binomialCoefficient(N,n)
            reply {
              UniqueEndMessage
            }
            exit()
        }
      }
    }

    val firstActor, secondActor, thirdActor:Doer = new Doer

    firstActor.start
    secondActor.start
    thirdActor.start
    firstActor ! ReaderMessage2
    secondActor ! ReaderMessage1
    thirdActor ! ReaderMessage3

    receive {
      case UniqueEndMessage =>
    }
    receive {
      case UniqueEndMessage =>
    }
    receive {
      case UniqueEndMessage =>
    }
    return((KOverk*bigOverLittle)/(Novern))
  }

  /**
   * Concurrent merge sort
   */
  implicit def IntIntLessThan(x:Category, y:Category) = {
    x.getHypergeometricDistribution() > y.getHypergeometricDistribution()
  }

  def mergeSort[T](theList:List[T])(implicit pred:(T, T) => Boolean):List[T] = {
    val m = theList.length / 2

    if(m == 0) {
      theList
    }
    else {
      def merge(left:List[T], right:List[T], atemp:List[T] = List()):List[T] = (left, right) match {
        case(Nil, _) => atemp ++ right
        case(_, Nil) => atemp ++ left
        case(l :: left1, r :: right1) =>
          if(pred(l, r)) {
            merge(left1, right, atemp:+ l)
          }
          else {
            merge(left, right1, atemp:+ r)
          }
      }
      val(l, r) = theList splitAt m
      merge(mergeSort(l), mergeSort(r))
    }
  }

  /**
   * Method that declares and prints out a sorted list of categories based on hyper-geometric distribution
   */
  def sortList():Unit = {
    myMod1CategoryList = myCategoryList.take(myCategoryList.size/2)
    myMod2CategoryList = myCategoryList.drop(myCategoryList.size/2)
    val hlactor, blactor = new Sorter

    hlactor.start
    blactor.start
    hlactor ! ReaderMessage1
    blactor ! ReaderMessage2

    receive {
      case UniqueEndMessage => 
    }
    receive {
      case UniqueEndMessage => 
    }

    combineLists()
    myCategoryList = mergeSort(myCategoryList)

    println("Hypergeometric distribution for all categories in sorted order: ")
    for(x <- myCategoryList) {
      println(x.getName() + " Geometric : " + x.getHypergeometricDistribution())
    }
  }

  /**
   * sort class that extends actors to merge
   */
  class Sorter extends Actor {
    def act {
      while(true) {
        receive {
          case ReaderMessage2 =>
            myMod2CategoryList = mergeSort(myMod2CategoryList)
            reply {
              UniqueEndMessage
            }
            exit()
          case ReaderMessage1 =>
            myMod1CategoryList = mergeSort(myMod1CategoryList)
            reply {
              UniqueEndMessage
            }
            exit()
        }
      }
    }
  }
}
