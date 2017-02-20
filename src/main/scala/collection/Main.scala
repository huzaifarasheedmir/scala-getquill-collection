package collection

import io.getquill.{CassandraAsyncContext, SnakeCase}
import io.getquill.context.cassandra.CassandraSessionContext

import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}



trait Encoders {
  this: CassandraSessionContext[_] =>

  implicit def listEncoder[T](implicit t: ClassTag[T]): Encoder[List[T]] =
    encoder((index, value, row) => row.setList(index, value.asJava, t.runtimeClass.asInstanceOf[Class[T]]))
}

trait Decoders {
  this: CassandraSessionContext[_] =>

  implicit def listDecoder[T](implicit t: ClassTag[T]): Decoder[List[T]] =
    decoder((index, row) => row.getList(index, t.runtimeClass.asInstanceOf[Class[T]]).asScala.toList)
}

object Main extends App{
  lazy val context = new CassandraAsyncContext[SnakeCase]("ctx")with Encoders with Decoders

  import context._

  val entry = new Fruits("2", List("Mango", "Kiwi"))

  implicit val movieSchemaMeta = schemaMeta[Fruits]("fruits", _.id -> "id", _.fruit_list-> "fruit_list")

  val insertFruits = quote {
    query[Fruits].insert(lift(entry)).ifNotExists
  }
  context.run(insertFruits)

  val getFruits = quote {
    query[Fruits].filter(row => row.id=="1")

  }

  context.run(getFruits).onComplete{
    case Success(lst) => println(lst)
    case Failure(exception)=> println(exception.getMessage)
  }

}


