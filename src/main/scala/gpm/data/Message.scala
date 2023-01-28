package gpm.data

import gpm.data.Message._
import org.apache.spark.graphx.VertexId

case class Message[T](flag: Byte,
                      data: Option[T],
                      dst: VertexId = 0,
                      step: Byte) extends Serializable {
    def setFlag(newFlag: Byte): Message[T] = {
        new Message[T](newFlag, data, dst, step)
    }

    def isDefined: Boolean = data.isDefined

    def isEmpty: Boolean = data.isEmpty || flag == EMPTY_MESSAGE

    def isRequest: Boolean = data.isEmpty && flag == REQUEST_MESSAGE

    def isInit: Boolean = data.isEmpty && flag == INIT_MESSAGE

    def isData: Boolean = data.isDefined && (flag == DATA_MESSAGE || flag == DATA_TO_CHILDREN || flag == DATA_TO_PARENTS)

    def isRemoval: Boolean = data.isDefined && flag == REMOVAL_MESSAGE

    def isToken: Boolean = data.isDefined && flag == TOKEN_MESSAGE

}

object Message {
    val INIT_MESSAGE: Byte = 0
    val REQUEST_MESSAGE: Byte = 1
    val DATA_MESSAGE: Byte = 2
    val REMOVAL_MESSAGE: Byte = 3
    val EMPTY_MESSAGE: Byte = 4
    val DATA_TO_CHILDREN: Byte = 5
    val DATA_TO_PARENTS: Byte = 6
    val TOKEN_MESSAGE: Byte = 7

    def empty[T]: Message[T] = Message(EMPTY_MESSAGE, None, -1, -1)

    def request[T]: Message[T] = Message(REQUEST_MESSAGE, None, 0, -1)

    def init[T]: Message[T] = Message[T](INIT_MESSAGE, None, 0, -1)
}