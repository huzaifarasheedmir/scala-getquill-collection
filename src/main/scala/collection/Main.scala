package collection

import com.google.common.reflect.TypeToken
import io.getquill.{CassandraAsyncContext, SnakeCase}
import io.getquill.context.cassandra.CassandraSessionContext

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

import scala.util.{Failure, Success}



trait Encoders {
  this: CassandraSessionContext[_] =>

  /*
  Will work List with any non collection data type
   */
  implicit def listEncoder[T](implicit t: ClassTag[T]): Encoder[List[T]] =
    encoder((index, value, row) => row.setList(index, value.asJava, t.runtimeClass.asInstanceOf[Class[T]]))


  /*
  Will work for list of Map[String, String]
   */
  val token = new TypeToken[java.util.Map[String, String]]() {}
  implicit val listMapEncoder: Encoder[List[java.util.Map[String, String]]] =
    encoder((index, value, row) => row.setList(index, value.asJava, token))


  /*
  Will work for Map[String, Sting] both key and value should be of same data types or explicitly
  specify the types
   */

  implicit def mapEncoder[T](implicit t: ClassTag[T]): Encoder[Map[T, T]] =
    encoder((index, value, row) => row.setMap(index, value.asJava))


}

trait Decoders {
  this: CassandraSessionContext[_] =>

  implicit def listDecoder[T](implicit t: ClassTag[T]): Decoder[List[T]] =
    decoder((index, row) => row.getList(index, t.runtimeClass.asInstanceOf[Class[T]]).asScala.toList)

  val token = new TypeToken[java.util.Map[String, String]]() {}
  implicit val listMapDecoder: Decoder[List[java.util.Map[String, String]]] =
    decoder((index, row) => row.getList(index, token).asScala.toList)

  implicit def mapDecoder[T](implicit t: ClassTag[T]): Decoder[Map[T, T]] =
    decoder((index, row) => row.getMap(index, t.runtimeClass.asInstanceOf[Class[T]], t.runtimeClass.asInstanceOf[Class[T]]).asScala.toMap)

}

object Main extends App{
  lazy val context = new CassandraAsyncContext[SnakeCase]("ctx")with Encoders with Decoders

  import context._

  val entry = new Fruits("2", List("Mango", "Kiwi"))

  val mixEntry = new Mix("1", List("a", "b"),Map("A" -> "a"), List(Map("A" -> "a").asJava, Map("B" -> "b").asJava))


  implicit val movieSchemaMeta = schemaMeta[Fruits]("fruits", _.id -> "id", _.fruit_list-> "fruit_list")

  val insertFruits = quote {
    query[Fruits].insert(lift(entry)).ifNotExists
  }
  context.run(insertFruits)

  val insertMix = quote {
    query[Mix].insert(lift(mixEntry)).ifNotExists
  }
  context.run(insertFruits)

  val getFruits = quote {
    query[Fruits].filter(row => row.id=="1")
  }
  context.run(getFruits).onComplete{
    case Success(lst) => println(lst)
    case Failure(exception)=> println(exception.getMessage)
  }

  val getMix = quote {
    query[Mix].take(10)
  }
  context.run(getMix).onComplete{
    case Success(lst) => println(lst)
    case Failure(exception)=> println(exception.getMessage)
  }

}


