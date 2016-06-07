package tools

import java.io.Serializable
import java.net.Socket

/**
  * Created by QQ on 2016/6/6.
  */
class LazyConn(createSocketConnection: () => Socket) extends Serializable {

  lazy val socketConn = createSocketConnection()
}

object LazyConn {

  def apply(address: String, port: Int): LazyConn = {

    val createSocket = () => {

      val socketConn = new Socket(address, port)

      sys.addShutdownHook {
        socketConn.close()
      }

      socketConn
    }

    new LazyConn(createSocket)
  }
}
