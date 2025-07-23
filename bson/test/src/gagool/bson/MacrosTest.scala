package gagool.bson

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.bson.*
import scala.util.Try
import BaseCodecs.given

private final case class Age(value: Int) extends AnyVal
private case class CustomerId(value: String)
private case class Customer(id: CustomerId, name: String)
private opaque type Email = String
private object Email:
  def apply(s: String): Email = s
  extension (e: Email) def value: String = e
private case class Address(street: String, number: Int)
private case class Person(name: String, age: Int, address: Address)
private sealed trait Shape
private case class Circle(radius: Double) extends Shape
private case class Rectangle(width: Double, height: Double) extends Shape

private type Parent2 = Circle | Address

private object ParentObject:
  sealed trait NumberOrString
  case class NumVal(n: Int) extends NumberOrString
  case class StrVal(s: String) extends NumberOrString

private object SingletonType

class MacroSpec extends AnyFlatSpec with Matchers:

  given BsonValueCodec[CustomerId] with
    def encode(in: CustomerId): BsonValue = new BsonString(in.value)
    def decode(in: BsonValue) = Try(CustomerId(in.asString().getValue))

  given BsonDocCodec[Customer] with
    def encode(c: Customer): BsonDocument =
      val doc = new BsonDocument()
      doc.put("id", summon[BsonValueCodec[CustomerId]].encode(c.id))
      doc.put("name", summon[BsonValueCodec[String]].encode(c.name))
      doc

    def decode(in: BsonDocument) = Try {
      val id = summon[BsonValueCodec[CustomerId]].decode(in.get("id")).get
      val name = summon[BsonValueCodec[String]].decode(in.get("name")).get
      Customer(id, name)
    }

  "List codec".should("round-trip a list of strings") in {
    val codec = summon[BsonValueCodec[List[String]]]
    val original = List("a", "b", "c")
    val encoded = codec.encode(original)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Map codec".should("round-trip a map of string->string") in {
    val codec = summon[BsonValueCodec[Map[String, String]]]
    val original = Map("foo" -> "bar", "baz" -> "qux")
    val encoded = codec.encode(original)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Customer codec".should("round-trip a case class") in {
    val codec = summon[BsonDocCodec[Customer]]
    val original = Customer(CustomerId("c1"), "Alice")
    val encoded = codec.encode(original)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Macros.valueClass".should("derive codec for simple value class") in {
    val codec = Codec.imap(Age.apply)(_.value)
    val original = Age(25)
    val encoded = codec.encode(original)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Macros.valueNewtype".should("derive codec for opaque type") in {
    val codec = Codec.imap(Email.apply)(_.value)
    val original = Email("test@example.com")
    val encoded = codec.encode(original)
    println("----------------" + original + encoded)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Macros.manual".should("derive codec for complex case class") in {
    given BsonDocCodec[Address] = Macros.manual[Address]
    val codec = Macros.manual[Person]

    val original = Person("Alice", 30, Address("Main St", 123))
    val encoded = codec.encode(original)
    println("----------------" + original + encoded)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Macros.singleton".should("derive codec for singleton type") in {
    val codec = Macros.singleton[SingletonType.type]
    val original = SingletonType
    val encoded = codec.encode(original)
    println("----------------" + original + encoded)
    val decoded = codec.decode(encoded).get
    decoded shouldEqual original
  }

  "Macros.sealedTrait".should("derive codec for sealed trait hierarchy") in {

    given BsonDocCodec[Circle] = Macros.manual[Circle]
    given BsonDocCodec[Rectangle] = Macros.manual[Rectangle]
    val codec = Macros.sealedTrait[Shape]

    val circle: Shape = Circle(5.0)
    val rectangle: Shape = Rectangle(4.0, 6.0)

    val encodedCircle = codec.encode(circle)
    println("----------------" + circle + encodedCircle)
    val encodedRect = codec.encode(rectangle)

    val decodedCircle = codec.decode(encodedCircle).get
    val decodedRect = codec.decode(encodedRect).get

    decodedCircle shouldEqual circle
    decodedRect shouldEqual rectangle
  }

  it.should("handle union types via sealed trait") in {
    import ParentObject.{NumVal, StrVal, NumberOrString}
    given BsonDocCodec[NumVal] = Macros.manual[NumVal]
    given BsonDocCodec[StrVal] = Macros.manual[StrVal]
    val codec = Macros.sealedTrait[NumberOrString]

    val num: NumberOrString = NumVal(42)
    val str: NumberOrString = StrVal("hello")

    val encodedNum = codec.encode(num)
    println(s"----------- $num $encodedNum")
    val encodedStr = codec.encode(str)

    val decodedNum = codec.decode(encodedNum).get
    val decodedStr = codec.decode(encodedStr).get

    decodedNum shouldEqual num
    decodedStr shouldEqual str
  }
