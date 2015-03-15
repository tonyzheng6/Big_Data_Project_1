import scala.actors.Actor
import scala.actors.Actor._
import scala.math.BigDecimal

/**
 * Created by Tony on 3/15/15.
 */
object HyperGeometricDistribution {

  /**
   * Method that calculates the hyper-geometric distribution with nested methods that calculates the binomial
   * coefficient and factorial
   */
  def hypergeometricDistribution(N: Int, K: Int, n: Int, k: Int): BigDecimal = {
    def factorial(n: BigDecimal, result: BigDecimal): BigDecimal = {
      if (n <= 1) {
        return result
      }
      else {
        factorial(n - 1, n * result)
      }
    }

    def reduxFactorial(n: BigDecimal, a: BigDecimal, result: BigDecimal): BigDecimal = {
    	if (n == a)
    	{
    		return result
    	}
    	if (n <= 1) {
    		return result
    	}
    	else {
    		reduxFactorial(n-1, a, n*result)
    	}
    }

    def binomialCoefficient(a: BigDecimal, b: BigDecimal): BigDecimal = {
      var bigdone, bigdtwo, bigdthree: BigDecimal = 0
      class biDoer extends Actor {
        def act {
          receive {
            case ReaderMessage2 =>
              bigdone = reduxFactorial(a,b,1)
              reply { UniqueEndMessage }
              exit()
            case ReaderMessage1 =>
              bigdtwo = factorial(a-b, 1)
              reply { UniqueEndMessage }
              exit()
            case ReaderMessage3 =>
              bigdthree = bigdone * 1/bigdtwo
              reply { UniqueEndMessage }
              exit()
          }
        }
      }
      val firstActor, secondActor, thirdActor: biDoer = new biDoer

      firstActor.start
      secondActor.start
      thirdActor.start
      firstActor ! ReaderMessage2
      secondActor ! ReaderMessage1
      receive { case UniqueEndMessage => }
      receive { case UniqueEndMessage => }
      thirdActor ! ReaderMessage3
      receive { case UniqueEndMessage => }

      return (bigdthree)
      //  return (factorial(a, 1) / (factorial(b, 1) * factorial((a - b),1)))
    } /////////
    //return ((binomialCoefficient(K, k) * binomialCoefficient((N - K), (n - k))) / binomialCoefficient(N, n))
    var KOverk: BigDecimal = 0
    var bigOverLittle: BigDecimal = 0
    var Novern: BigDecimal = 0

    class Doer extends Actor {
      def act {
        receive {
          case ReaderMessage2 =>
            KOverk = binomialCoefficient(K, k)
            reply { UniqueEndMessage }
            exit()
          case ReaderMessage1 =>
            bigOverLittle = binomialCoefficient((N - K), (n - k))
            reply { UniqueEndMessage }
            exit()
          case ReaderMessage3 =>
            Novern = binomialCoefficient(N, n)
            reply { UniqueEndMessage }
            exit()
        }
      }
    }

    val firstActor, secondActor, thirdActor: Doer = new Doer

    firstActor.start
    secondActor.start
    thirdActor.start
    firstActor ! ReaderMessage2
    secondActor ! ReaderMessage1
    thirdActor ! ReaderMessage3

    receive { case UniqueEndMessage => }
    receive { case UniqueEndMessage => }
    receive { case UniqueEndMessage => }

    return ((KOverk * bigOverLittle) / (Novern))
  }
}
