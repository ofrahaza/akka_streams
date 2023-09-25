import akka.{NotUsed, actor}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.{ClosedShape, FlowShape, Graph, SinkShape, SourceShape, UniformFanOutShape}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}
import akka_typed.CalculatorRepository.{Result, getLatestsOffsetAndResult, updatedResultAndOffset}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt

object akka_typed {

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide{
    sealed trait Command
    case class Add(amount: Int) extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id:Int, amount: Int) extends Event
    case class Multiplied(id:Int, amount: Int) extends Event
    case class Divided(id:Int, amount: Int) extends Event

    final case class State(value:Int) extends CborSerialization
    {
      def add(amount: Int): State = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State = copy(value = value / amount)
    }

    object State{
      val empty = State(0)
    }

    def handleCommand(
                       persistenceId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }
  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {
    implicit val ec: ExecutionContextExecutor = system.executionContext
    implicit val materializer: actor.ActorSystem = system.classicSystem
    implicit val session: SlickSession = SlickSession.forConfig("slick-postgres")

    var res: Result = getLatestsOffsetAndResult
    val startOffset: Long = if (res.offset == 1) 1 else res.offset + 1
    val readJournal: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    def updateState(event: EventEnvelope): Result = {
      event.event match {
        case Added(_, amount) =>
          val newRes = Result(res.state + amount, event.sequenceNr)
          updatedResultAndOffset(newRes)
          println(s"Added: ${newRes.state}")
          newRes
        case Multiplied(_, amount) =>
          val result = Result(res.state * amount, event.sequenceNr)
          updatedResultAndOffset(result)
          println(s"Multiplied:  ${result.state}")
          result
        case Divided(_, amount) =>
          val result = Result(res.state / amount, event.sequenceNr)
          updatedResultAndOffset(result)
          println(s"Divided:  ${result.state}")
          result
      }
    }

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        val input: SourceShape[EventEnvelope] = builder.add(source)
        val update: FlowShape[EventEnvelope, Result] = builder.add(Flow[EventEnvelope].map(e => updateState(e)))
        val localSaveOutput: SinkShape[Result] = builder.add(Sink.foreach[Result] {
          r =>
            res = res.copy(state = r.state)
        })

        val dbSaveOutput: SinkShape[Result] = builder.add(
          Slick.sink(updatedResultAndOffset)
        )
        val broadcast: UniformFanOutShape[Result, Result] = builder.add(Broadcast[Result](2))

        import GraphDSL.Implicits._
        input ~> update ~> broadcast

        broadcast.out(0) ~> dbSaveOutput
        broadcast.out(1) ~> localSaveOutput

        ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  object CalculatorRepository {

    case class Result(state: Double, offset: Long)

    def getLatestsOffsetAndResult(implicit executionContext: ExecutionContextExecutor, session: SlickSession): Result = {
      val query = sql"""select calculated_value, write_side_offset from public.result where id = 1;""".as[(Double, Long)].headOption
      val dbrun: Future[Option[Result]] = session.db.run(query).map(v => v.flatMap(r => Some(Result(r._1, r._2))))
      Await.result(dbrun, 5000.millis)
    }.getOrElse(throw new RuntimeException("error"))

    def updatedResultAndOffset = (res: Result) => sqlu"update public.result set calculated_value = ${res.state}, write_side_offset = ${res.offset} where id = 1"
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup {
      ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! Add(10)
        writeAcorRef ! Multiply(2)
        writeAcorRef ! Divide(5)

        Behaviors.same
    }

  def execute(command: Command): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! command
      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    TypedCalculatorReadSide(system)
  }
}