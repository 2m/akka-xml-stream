package com.drewhk.stream.xml

import javax.xml.namespace.QName
import javax.xml.stream.events.{Attribute, Namespace}

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.fasterxml.aalto.{AsyncByteArrayFeeder, AsyncXMLInputFactory, AsyncXMLStreamReader}
import com.fasterxml.aalto.stax.InputFactoryImpl

import scala.annotation.tailrec
import scala.collection.immutable

object Xml {

  sealed trait ParseEvent
  sealed trait TextEvent extends ParseEvent {
    def text: String
  }

  case object StartDocument extends ParseEvent
  case object EndDocument extends ParseEvent
  final case class StartElement(localName: String, attributes: Map[String, String]) extends ParseEvent
  final case class EndElement(localName: String) extends ParseEvent
  final case class Characters(text: String) extends TextEvent
  final case class ProcesssingInstruction(target: Option[String], data: Option[String]) extends ParseEvent
  final case class Comment(text: String) extends ParseEvent
  final case class CData(text: String) extends TextEvent

  val parser: Flow[ByteString, ParseEvent, NotUsed] =
    Flow.fromGraph(new StreamingXmlParser)

  def coalesce(maximumTextLength: Int): Flow[ParseEvent, ParseEvent, NotUsed] =
    Flow.fromGraph(new Coalesce(maximumTextLength))

  private class StreamingXmlParser extends GraphStage[FlowShape[ByteString, ParseEvent]] {
    val in: Inlet[ByteString] = Inlet("XML.Parser.in")
    val out: Outlet[ParseEvent] = Outlet("XML.Parser.out")
    override val shape: FlowShape[ByteString, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {

        import javax.xml.stream.XMLStreamConstants

        private[this] val feeder: AsyncXMLInputFactory = new InputFactoryImpl()
        private[this] val parser: AsyncXMLStreamReader[AsyncByteArrayFeeder] = feeder.createAsyncFor(Array.empty)

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          val array = grab(in).toArray
          parser.getInputFeeder.feedInput(array, 0, array.length)
          advanceParser()
        }

        override def onPull(): Unit = advanceParser()

        override def onUpstreamFinish(): Unit = {
          parser.getInputFeeder.endOfInput()
          if (!parser.hasNext) completeStage()
        }

        @tailrec private def advanceParser(): Unit = {
          if (parser.hasNext) {
            parser.next() match {
              case AsyncXMLStreamReader.EVENT_INCOMPLETE =>
                if (!isClosed(in)) pull(in)
                else completeStage() // Fail instead??

              case XMLStreamConstants.START_DOCUMENT =>
                push(out, StartDocument)

              case XMLStreamConstants.END_DOCUMENT =>
                push(out, EndDocument)
                completeStage()

              case XMLStreamConstants.START_ELEMENT =>
                val attributes = (0 until parser.getAttributeCount).map { i =>
                  parser.getAttributeLocalName(i) -> parser.getAttributeValue(i)
                }.toMap

                push(out, StartElement(parser.getLocalName, attributes))

              case XMLStreamConstants.END_ELEMENT =>
                push(out, EndElement(parser.getLocalName))

              case XMLStreamConstants.CHARACTERS =>
                push(out, Characters(parser.getText))

              case XMLStreamConstants.PROCESSING_INSTRUCTION =>
                push(out, ProcesssingInstruction(Option(parser.getPITarget), Option(parser.getPIData)))

              case XMLStreamConstants.COMMENT =>
                push(out, Comment(parser.getText))

              case XMLStreamConstants.CDATA =>
                push(out, CData(parser.getText))

              // Do not support DTD, SPACE, NAMESPACE, NOTATION_DECLARATION, ENTITY_DECLARATION, PROCESSING_INSTRUCTION
              // ATTRIBUTE is handled in START_ELEMENT implicitly

              case x =>
                if (parser.hasNext) advanceParser()
                else completeStage()
            }
          } else completeStage()
        }
      }
  }

  private class Coalesce(maximumTextLength: Int) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
    val in: Inlet[ParseEvent] = Inlet("XML.Coalesce.in")
    val out: Outlet[ParseEvent] = Outlet("XML.Coalesce.out")
    override val shape: FlowShape[ParseEvent, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var isBuffering = false
        private var buffer = new StringBuilder

        override def onPush(): Unit = grab(in) match {
          case t: TextEvent =>
            if (t.text.length + buffer.length > maximumTextLength)
              failStage(new IllegalStateException(s"Too long character sequence, maximum is $maximumTextLength but got " +
                s"${t.text.length + buffer.length - maximumTextLength} more "))
            else {
              buffer.append(t.text)
              isBuffering = true
              pull(in)
            }
          case other =>
            if (isBuffering) {
              val coalesced = buffer.toString()
              isBuffering = false
              buffer.clear()
              emit(out, Characters(coalesced), () => emit(out, other))
            } else {
              push(out, other)
            }
        }

        override def onPull(): Unit = pull(in)

        setHandlers(in, out, this)
      }
  }

}
