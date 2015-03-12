/**
 * Title:       Category.scala
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
 * Notes:       Concurrent version
 */

class Category {
  private var name:String = ""
  private var count:Int = 1
  private var hypergeometricDistribution:BigDecimal = 0

  /**
   * Setter for name
   */
  def setName(name:String):Unit = {
    this.name = name
  }

  /**
   * Overloaded method that compares the hypergeometric distribution
   */
  def <=(other:Category):Boolean = {
  	if(this.hypergeometricDistribution <= other.hypergeometricDistribution) {
      return true
    }
  	else {
      return false
    }
  }

  /**
   * Getter for name
   */
  def getName():String = {
    return name
  }

  /**
   * Setter for count
   */
  def setCount(count:Int):Unit = {
    this.count = count
  }

  /**
   * Getter for count
   */
  def getCount():Int = {
    return count
  }

  /**
   * Overloaded method that compares their counts
   */
  def compare(other:Category) = {
  	if(other.name == this.name) {
  		this.count += other.count
  	}
  }

  /**
   * Setter for hypergeometricDistribution
   */
  def setHypergeometricDistribution(hypergeometricDistribution:BigDecimal):Unit = {
    this.hypergeometricDistribution = hypergeometricDistribution
  }

  /**
   * Getter for hypergeometricDistribution
   */
  def getHypergeometricDistribution():BigDecimal = {
    return hypergeometricDistribution
  }

  /**
   * Method to increment count
   */
  def incrementCount():Unit = {
    count += 1
  }
}
