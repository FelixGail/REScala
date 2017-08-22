package example

import rescala._
import retier.communicator.tcp._
import retier.registry.{Binding, Registry}
import retier.serializer.upickle._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import rescala.fullmv.transmitter.SignalTransmittable._

object Bindings1 {
  val eventBinding = Binding [Evt[Int]]("listAdd")
  val variableBinding1 = Binding[Signal[List[Int]]]("variable")
}
//This method represents the server
object Server extends App {

  var testList1 = Var( List(1,2,3))
  def add(x: Int) = { testList1()= testList1.now :+ x}

  val e0 =  Evt[Int]
  e0 += add
  e0(10)

  val registry = new Registry
  registry.listen(TCP(1099))

  registry.bind(Bindings1.variableBinding1)(testList1)
  registry.bind(Bindings1.eventBinding)(e0)

  testList1 observe println

   while (System.in.available() == 0) {
     Thread.sleep(1000)
   }
  registry.terminate()
}
// This method represents a client. Multiple clients may be active at a time.
object Client extends App {
  val registry = new Registry
  val remote = Await result (registry.request(TCP("localhost", 1099)), Duration.Inf)

  import scala.concurrent.ExecutionContext.Implicits._

  val listOnServer: Signal[List[Int]] = Await result (registry.lookup(Bindings1.variableBinding1, remote), Duration.Inf)
  listOnServer observe println

  var input =""
  var continueProgram = true
  //val e0 :  Future[Evt[Int]] = registry.lookup(Bindings1.eventBinding, remote)
  while (continueProgram) {
    println("enter \"add\" to add a vaule or \"end\" to end")
    input = scala.io.StdIn.readLine()
    if (input == "end"){
      continueProgram = false
    }else if (input == "add"){
      println("enter a value")
      input = scala.io.StdIn.readLine()
      try
        {
          //e0(1)
        }
      catch
      {
        case e:Exception => println("please enter a valid number")
      }
    }
    Thread.sleep(10)
  }


  registry.terminate()
}
