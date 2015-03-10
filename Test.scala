/**
 * Title:       Test.scala
 * Created by:  Tony on 2/27/15.
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
 *              To compile: scalac *.scala // this doesn't work on glab but it should
 *              To run:     scala CalculateScores input.txt // the above doesn't work so this wouldn't either
 * Notes:       Concurrent Version
 */

import scala.actors.Actor
import scala.actors.Actor._
import scala.io.Source
import scala.collection.mutable._
case object ReaderMessageOdd
case object ReaderMessageEven
case object UniqueEndMessage

class Test(aFile:String) {
  private type T2 = Tuple2[Double, String]
  private var myPQE, myPQO, myPQF = PriorityQueue[T2]()(Ordering.by(doThis))
  private var numK, populationSize, draws:Int = 0
  private var myCategoryList, myOddCategoryList, myEvenCategoryList:List[Category] = List()
  private var myStringListOdd, myStringListEven:List[String] = List()
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
      case e: Exception => println("Exception caught: " + e)
        getK()
    }

    println()
  }

  def run():Unit = 
  {
  	var count:Int = 0
  	val firstActor, secondActor:Readers = new Readers

  	firstActor.start
  	secondActor.start
  	firstActor ! ReaderMessageEven
  	secondActor ! ReaderMessageOdd

    receive {
  		case UniqueEndMessage => count += 1
  	}
  	receive {
  		case UniqueEndMessage => count += 1
  	}

    combineQueues()
  }

  class Readers extends Actor {
		def act {
  		while (true) {
			  receive {
				  case ReaderMessageEven =>
				    runEven()
				    reply {
              UniqueEndMessage
            }
				    exit()
				  case ReaderMessageOdd =>
            runOdd()
				    reply {
              UniqueEndMessage
            }
            exit()
        }
      }
    }
  }

  /**
   * Method that streams the file input and creates the binary max heap of the data points based on their values
   */
  def runOdd():Unit = {
    var count, temp:Int = 0
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
  	 	if (count % 2 == 1) {
        myPQO.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))
        if(temp <= numK) {
           checkIfUniqueOdd((line.split('\t')(0).toDouble, line.split('\t')(1)))
     		   temp += 1
    		}
			  else {
          minimum = myPQO.min
          checkIfUniqueOdd((line.split('\t')(0).toDouble, line.split('\t')(1)))
      	  myPQO = myPQO.filterNot(it => it == minimum)
      	}
      }
      count += 1
  	}
  }

  def runEven():Unit = {
    var temp:Int = 0;
    var minimum:T2 = (0, "")

    for(line <- Source.fromFile(fileName).getLines()) {
  	 	if(populationSize % 2 == 0) {
  	 	  myPQE.enqueue((line.split('\t')(0).toDouble, line.split('\t')(1)))
        if(temp <= numK) {
          checkIfUniqueEven((line.split('\t')(0).toDouble, line.split('\t')(1)))
        	temp += 1
    		}
      	else {
          minimum = myPQE.min
          checkIfUniqueEven((line.split('\t')(0).toDouble, line.split('\t')(1)))
        	myPQE = myPQE.filterNot(it => it == minimum)
     		}
     	}
      populationSize += 1
    }
  }

  def combineQueues():Unit = {
    combineLists()
    var count:Int = numK
    var temp:Int = 0

    while(temp == 0) {
      if(myPQE.size != 0) {
        var eventemp = myPQE.dequeue
        myPQF += eventemp
      }
      else if(myPQO.size != 0) {
        var oddtemp = myPQO.dequeue
        myPQF += oddtemp
      }
      else {
        temp = 1
      }
    }

    while(myPQF.size != numK) {
      myPQF = myPQF.filterNot(it => it == myPQF.min)
    }
  }

  /**
   * Method that checks if the file input belongs to a category and increments the successes if it does, else creates a
   * new category and places it into a list
   * private var myCategoryList:List[Category] = List()
   * private var myOddCategoryList:List[Category] = List()
   */
  def checkIfUniqueOdd(x:T2):Unit = {
    val categoryName = x._2

    if(!myStringListOdd.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringListOdd = myStringListOdd :+ categoryName
      myOddCategoryList = myOddCategoryList :+ myCategory
    }
    else {
      myOddCategoryList(myStringListOdd.indexOf(categoryName)).incrementCount()
    }
  }

  def checkIfUniqueEven(x:T2):Unit = {
    val categoryName:String = x._2

    if(!myStringListEven.contains(categoryName)) {
      val myCategory = new Category()
      myCategory.setName(categoryName)
      myStringListEven = myStringListEven :+ categoryName
      myCategoryList = myCategoryList :+ myCategory
    }
    else {
      myCategoryList(myStringListEven.indexOf(categoryName)).incrementCount()
    }
  }

  def combineLists():Unit = {
  	var countOdd, countEven:Int = 0

  	while(countOdd < myOddCategoryList.size) {
  		var tempname = myOddCategoryList(countOdd).getName
  		var tempcount = myOddCategoryList(countOdd).getCount

      if(!myStringListEven.contains(tempname)) {
	      val myCategory = new Category()
	      myCategory.setName(tempname)
	      myCategory.setCount(tempcount)
	      myCategoryList = myCategoryList :+ myCategory  	
  		}
  		else {
  		  while(countEven < myCategoryList.size) {
  		  	myCategoryList(countEven).compare(myOddCategoryList(countOdd))
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
  def printAll(myHeap:PriorityQueue[T2]):Unit = {
    printPQ(myHeap)

    def printPQ(it:PriorityQueue[T2]):Unit = {
      if (it.isEmpty) {
        return
      }

      println(it.head._1)
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
      println("FOR: " + x.getName())
      println("Population size is: " + populationSize)
      println("Total success states in population is: " + x.getCount())
      println("Number of draws is: " + draws)
      println("Number of draws in top " + numK + " is: " + getSuccesses(x))
      x.setHypergeometricDistribution(hypergeometricDistribution(populationSize, x.getCount(), draws, getSuccesses(x)))
      println("Hypergeometric distribution is: " + x.getHypergeometricDistribution())
      println()
    }
  }

  /**
   * Method that calculates the hyper-geometric distribution with nested methods that calculates the binomial
   * coefficient and factorial
   */
  def hypergeometricDistribution(N:Int, K:Int, n:Int, k:Int):Double = {
    def factorial(number:Int):Double = {
      def factorialWithAccumulator(accumulator: Double, number: Double): Double = {
        if(number <= 1) {
          return accumulator
        }
        else {
          factorialWithAccumulator(accumulator * number, number - 1)
        }
      }

      return factorialWithAccumulator(1, number)
    }

    def binomialCoefficient(a:Int, b:Int):Double = {
      return (factorial(a) / (factorial(b) * factorial(a - b)))
    }

    return ((binomialCoefficient(K, k) * binomialCoefficient((N - K), (n - k))) / binomialCoefficient(N, n))
  }

  /**
   * Merge sort taken from online source, able to use if made concurrent
   */
  implicit def IntIntLessThan(x:Category, y:Category) = {
    x.getHypergeometricDistribution() > y.getHypergeometricDistribution()
  }

  def mergeSort[T](xs:List[T])(implicit pred:(T, T) => Boolean):List[T] = {
    val m = xs.length / 2

    if(m == 0) {
      xs
    }
    else {
      def merge(ls:List[T], rs:List[T], acc:List[T] = List()):List[T] = (ls, rs) match {
        case(Nil, _) => acc ++ rs
        case(_, Nil) => acc ++ ls
        case(l :: ls1, r :: rs1) =>
          if(pred(l, r)) {
            merge(ls1, rs, acc :+ l)
          }
          else {
            merge(ls, rs1, acc :+ r)
          }
      }
      val(l, r) = xs splitAt m
      merge(mergeSort(l), mergeSort(r))
    }
  }

  /**
   * Method that declares and prints out a sorted list of categories based on hyper-geometric distribution
   */
  def sortList():Unit = {
    myOddCategoryList = myCategoryList.take(myCategoryList.size/2)
    myEvenCategoryList = myCategoryList.drop(myCategoryList.size/2)
    val hlactor, blactor = new Sorter
    hlactor.start
    blactor.start
    hlactor ! ReaderMessageOdd
    blactor ! ReaderMessageEven

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

  class Sorter extends Actor {
    def act {
      while(true) {
        receive {
          case ReaderMessageEven =>
            myEvenCategoryList = mergeSort(myEvenCategoryList)
            reply {
              UniqueEndMessage
            }
            exit()
          case ReaderMessageOdd =>
            myOddCategoryList = mergeSort(myOddCategoryList)
            reply {
              UniqueEndMessage
            }
            exit()
        }
      }
    }
  }
}
