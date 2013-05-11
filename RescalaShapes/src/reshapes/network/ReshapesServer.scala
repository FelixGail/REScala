package reshapes.network

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.ConnectException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket

import scala.actors.Actor
import scala.collection.mutable.MutableList
import scala.xml.XML

import reshapes.Reshapes
import reshapes.figures.Shape

object ReshapesServer {

  var clients = MutableList[(InetAddress, Int)]()
  var currentShapes = List[Shape]()

  def main(args: Array[String]): Unit = {
    if (args.size >= 2) {
      val commandThreadPort = args(0).toInt
      val updateThreadPort = args(1).toInt

      new CommandThread(commandThreadPort).start()
      new UpdateThread(updateThreadPort).start()
    } else {
      println("invalid number of arguments")
    }
  }

  /**
   * Registers a client to the server if not already registered.
   */
  def registerClient(inetAddress: InetAddress, port: Int) = {
    if (!(clients exists (client => client._1 == inetAddress && client._2 == port))) {
      clients += ((inetAddress, port))
      println("ReshapesServer register new client (%s, %d)".format(inetAddress, port))
      print("\t registered clients: ")
      clients map (client => println("(%s, %d)".format(client._1, client._2)))
      print("\n")
      sendToClient(currentShapes, (inetAddress, port))
    }
  }

  /**
   * Removes a client so he no longer receives updates.
   */
  def removeClient(client: (InetAddress, Int)) = {
    println("ReshapesServer removing client " + client.toString())
    clients = clients filter (c => c._1 != client._1 && c._2 != client._2)
  }

  /**
   * Sends the given shapes to all registered clients except the original sender
   */
  def sendUpdateToClients(shapes: List[Shape], sender: (InetAddress, Int)) = {
    currentShapes = shapes
    for (client <- clients) {
      if (client._1 != sender._1 ||
        (client._1 == sender._1 && client._2 != sender._2)) {
        if (!sendToClient(shapes, client)) {
          removeClient(sender)
        }
      }
    }
  }

  /**
   * Sends shapes to a client.
   * returns true if shapes where successfully send, false otherwise (connection refused to client)
   */
  def sendToClient(shapes: List[Shape], client: (InetAddress, Int)): Boolean = {
    try {
      val socket = new Socket(client._1, client._2)
      val writer = new OutputStreamWriter(socket.getOutputStream)
      XML.write(writer, Shape.serialize(shapes), "", false, null)
      writer.close
      socket.close
    } catch {
      case e: ConnectException =>
        return false
    }

    true
  }
}

/**
 * Listens to string commands:
 * 	register [port] - registers a new client
 */
class CommandThread(port: Int) extends Actor {
  def act() {
    println("start CommandThread")
    val listener = new ServerSocket(port)
    while (true) {
      val clientSocket = listener.accept()
      val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))

      val command = in.readLine()
      println("CommandThread new command: '%s'".format(command))
      command match {
        case registerCmd if registerCmd.toLowerCase().startsWith("register ") =>
          val clientPort = registerCmd.split(" ")(1).toInt
          ReshapesServer.registerClient(clientSocket.getInetAddress(), clientPort)
        case _ => println("unkown command: " + command)
      }

      in.close()
      clientSocket.close()
    }
    listener.close()
  }
}

/**
 * Listens to shapes updates
 */
class UpdateThread(port: Int) extends Actor {
  def act() {
    println("start UpdateThread")
    val listener = new ServerSocket(port)
    while (true) {
      val socket = listener.accept
      val shapes = Shape.deserialize(XML.load(socket.getInputStream), Reshapes.drawingSpaceState)
      ReshapesServer.sendUpdateToClients(shapes, (socket.getInetAddress, socket.getPort))
      socket.close
    }
    listener.close()
  }
}

