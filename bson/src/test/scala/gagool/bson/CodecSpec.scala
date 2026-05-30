package gagool.bson

import gagool.bson.BaseCodecs.given
import gagool.bson.TestUtil.given
import org.bson.*
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class CodecSpec extends AnyFunSpec with Matchers {

  // Define some test data types for variance testing
  sealed trait Animal
  case class Dog(name: String) extends Animal
  case class Cat(name: String) extends Animal

  describe("Encoder") {
    it("should encode a value correctly") {
      val stringEncoder: BsonValueEncoder[String] = BaseCodecs.stringCodec
      stringEncoder.encode("hello") shouldBe new BsonString("hello")
    }

    it("should support contramap for input type transformation") {
      // Encode an Int as a String, then contramap to encode a Double
      val intEncoder: BsonValueEncoder[Int] = BaseCodecs.intCodec
      val doubleToIntEncoder: BsonValueEncoder[Double] =
        intEncoder.contramap(_.toInt)
      doubleToIntEncoder.encode(3.14) shouldBe new BsonInt32(3)
    }

    it("should support contravariance for input type (Animal/Dog example)") {
      // An encoder for Animal can encode a Dog
      val animalNameEncoder: Encoder[Animal, BsonValue] = {
        case Dog(name) => new BsonString(s"Dog: $name")
        case Cat(name) => new BsonString(s"Cat: $name")
      }

      val dog = Dog("Buddy")
      val encodedDog =
        animalNameEncoder.encode(dog) // This should compile and work
      val _ = encodedDog shouldBe new BsonString("Dog: Buddy")

      // Thanks to contravariance, an Encoder[Animal, BsonValue] can also be assigned to a variable of type Encoder[Dog, BsonValue] directly
      val dogEncoderFromAnimalDirect: Encoder[Dog, BsonValue] =
        animalNameEncoder
      val _ = dogEncoderFromAnimalDirect.encode(dog) shouldBe new BsonString(
        "Dog: Buddy"
      )

      val dogEncoderFromAnimalContramap: Encoder[Dog, BsonValue] =
        animalNameEncoder.contramap(identity)
      dogEncoderFromAnimalContramap.encode(dog) shouldBe new BsonString(
        "Dog: Buddy"
      )
    }

    it("should support covariance for output type") {
      val intEncoder: Encoder[Int, BsonInt32] = (in: Int) => new BsonInt32(in)
      // An Encoder[Int, BsonInt32] can be used as Encoder[Int, BsonValue] because BsonValue is a supertype of BsonInt32
      val intBsonValueEncoder: Encoder[Int, BsonValue] = intEncoder
      intBsonValueEncoder.encode(10) shouldBe new BsonInt32(10)
    }
  }

  describe("Decoder") {
    it("should decode a value correctly") {
      val stringDecoder: BsonValueDecoder[String] = BaseCodecs.stringCodec
      val _ =
        stringDecoder.decode(new BsonString("hello")) shouldBe Success("hello")
      val _ = stringDecoder.decode(new BsonInt32(123)) shouldBe a[Failure[?]]
    }

    it("should support map for output type transformation") {
      val intDecoder: BsonValueDecoder[Int] = BaseCodecs.intCodec
      val stringToIntDecoder: BsonValueDecoder[String] =
        intDecoder.map(_.toString)
      stringToIntDecoder.decode(new BsonInt32(123)) shouldBe Success("123")
    }

    it("should support flatMap for monadic composition") {
      val intDecoder: BsonValueDecoder[Int] = BaseCodecs.intCodec
      val positiveIntDecoder: BsonValueDecoder[Int] = intDecoder.flatMap { i =>
        if i > 0 then Success(i)
        else Failure(new IllegalArgumentException("Negative int"))
      }
      val _ = positiveIntDecoder.decode(new BsonInt32(5)) shouldBe Success(5)
      positiveIntDecoder.decode(new BsonInt32(-5)) shouldBe a[Failure[?]]
    }

    it("should support orElse for fallback decoding") {
      val intDecoder: BsonValueDecoder[Int] = BaseCodecs.intCodec
      val fallbackDecoder: BsonValueDecoder[Int] = intDecoder.orElse(Success(0))

      val _ = fallbackDecoder.decode(new BsonInt32(5)) shouldBe Success(5)
      fallbackDecoder.decode(new BsonString("not an int")) shouldBe Success(
        0
      ) // Fallback activated
    }

    it("should support covariance for output type (Animal/Dog example)") {
      // A decoder for Dog can be used where a decoder for Animal is expected (covariance in A)
      val dogDecoder: Decoder[Dog, BsonValue] = {
        case s: BsonString if s.getValue.startsWith("Dog:") =>
          Success(Dog(s.getValue.drop(4).trim))
        case _ => Failure(new IllegalArgumentException("Not a dog string"))
      }

      val animalDecoder: Decoder[Animal, BsonValue] =
        dogDecoder // Covariance in A allows this direct assignment
      val _ =
        animalDecoder.decode(new BsonString("Dog: Fido")) shouldBe Success(
          Dog("Fido")
        )
      animalDecoder
        .decode(new BsonString("Cat: Whiskers")) shouldBe a[Failure[?]]
    }

    it("should support contravariance for input type") {

      val stringBsonValueDecoder = BaseCodecs.stringCodec
      val _ =
        stringBsonValueDecoder.decode(new BsonString("test")) shouldBe Success(
          "test"
        )
      val _ =
        stringBsonValueDecoder.decode(new BsonInt32(1)) shouldBe a[Failure[?]]
    }
  }

  describe("Codec") {
    it("should create a codec from encode and decode functions") {
      val customCodec: BsonValueCodec[Int] = Codec(
        i => new BsonInt32(i * 2),
        {
          case i: BsonInt32 => Success(i.getValue / 2)
          case _ => Failure(new IllegalArgumentException("Not an int"))
        }
      )

      val _ = customCodec.encode(5) shouldBe new BsonInt32(10)
      val _ = customCodec.decode(new BsonInt32(10)) shouldBe Success(5)
    }

    it("should support imap for bidirectional transformation") {
      // Codec for String, transform to MyStringWrapper
      case class MyStringWrapper(value: String)
      val stringCodec: BsonValueCodec[String] = BaseCodecs.stringCodec

      val wrapperCodec: BsonValueCodec[MyStringWrapper] = stringCodec.imap(
        MyStringWrapper.apply,
        _.value
      )

      val wrapper = MyStringWrapper("WrappedText")
      val _ =
        wrapperCodec.encode(wrapper) shouldBe new BsonString("WrappedText")
      val _ =
        wrapperCodec.decode(new BsonString("DecodedText")) shouldBe Success(
          MyStringWrapper("DecodedText")
        )
    }
  }

  describe("BaseCodecs") {
    it("should encode and decode Set correctly") {
      val setStringCodec = BaseCodecs.setCodec[String]
      val originalSet = Set("a", "b", "c")
      val encoded = setStringCodec.encode(originalSet).asInstanceOf[BsonArray]
      val _ = encoded.getValues.asScala
        .map(_.asString().getValue)
        .toSet shouldBe originalSet
      val _ = setStringCodec.decode(encoded) shouldBe Success(originalSet)
    }

    it("should encode and decode Option correctly") {
      val optionIntCodec = BaseCodecs.optionCodec[Int]
      val _ = optionIntCodec.encode(Some(123)) shouldBe new BsonInt32(123)
      val _ = optionIntCodec.encode(None) shouldBe BsonNull.VALUE
      val _ =
        optionIntCodec.decode(new BsonInt32(123)) shouldBe Success(Some(123))
      val _ = optionIntCodec.decode(BsonNull.VALUE) shouldBe Success(None)
    }

    it("should encode and decode List correctly") {
      val listIntCodec = BaseCodecs.listCodec[Int]
      val originalList = List(1, 2, 3)
      val encoded = listIntCodec.encode(originalList).asInstanceOf[BsonArray]
      val _ = encoded.getValues.asScala
        .map(_.asInt32().getValue)
        .toList shouldBe originalList
      val _ = listIntCodec.decode(encoded) shouldBe Success(originalList)
    }

    it("should encode and decode Map correctly") {
      val mapStringCodec = BaseCodecs.mapCodec[String]
      val originalMap = Map("key1" -> "value1", "key2" -> "value2")
      val encoded =
        mapStringCodec.encode(originalMap).asInstanceOf[BsonDocument]
      val decodedMap = encoded
        .entrySet()
        .asScala
        .map(e => e.getKey -> e.getValue.asString().getValue)
        .toMap
      val _ = decodedMap shouldBe originalMap
      val _ = mapStringCodec.decode(encoded) shouldBe Success(originalMap)
    }

    it("should handle mixed numeric types for intCodec") {
      val intCodec = BaseCodecs.intCodec
      val _ = intCodec.decode(new BsonInt32(10)) shouldBe Success(10)
      val _ = intCodec.decode(new BsonInt64(10L)) shouldBe Success(
        10
      ) // truncation is fine for this test
      val _ = intCodec.decode(new BsonDouble(10.5)) shouldBe Success(
        10
      ) // truncation is fine for this test
      val _ = intCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for longCodec") {
      val longCodec = BaseCodecs.longCodec
      val _ = longCodec.decode(new BsonInt64(100L)) shouldBe Success(100L)
      val _ = longCodec.decode(new BsonInt32(100)) shouldBe Success(100L)
      val _ = longCodec.decode(new BsonDouble(100.5)) shouldBe Success(100L)
      val _ = longCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for floatCodec") {
      val floatCodec = BaseCodecs.floatCodec
      val _ = floatCodec.decode(new BsonDouble(10.5f)) shouldBe Success(10.5f)
      val _ = floatCodec.decode(new BsonInt32(10)) shouldBe Success(10.0f)
      val _ = floatCodec.decode(new BsonInt64(10L)) shouldBe Success(10.0f)
      val _ = floatCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for doubleCodec") {
      val doubleCodec = BaseCodecs.doubleCodec
      val _ = doubleCodec.decode(new BsonDouble(10.5)) shouldBe Success(10.5)
      val _ = doubleCodec.decode(new BsonInt32(10)) shouldBe Success(10.0)
      val _ = doubleCodec.decode(new BsonInt64(10L)) shouldBe Success(10.0)
      val _ = doubleCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle BsonNull for stringCodec") {
      val stringCodec = BaseCodecs.stringCodec
      stringCodec.decode(BsonNull.VALUE) shouldBe a[Failure[?]]
    }

    it("should handle BsonBoolean for boolCodec") {
      val boolCodec = BaseCodecs.boolCodec
      val _ = boolCodec.encode(true) shouldBe new BsonBoolean(true)
      val _ = boolCodec.decode(new BsonBoolean(false)) shouldBe Success(false)
      val _ = boolCodec.decode(new BsonInt32(1)) shouldBe a[Failure[?]]
    }
  }

  describe("Codec.fromEnum") {
    enum Color:
      case Red, Green, Blue

    it("should create codec from enum using values array") {
      val colorCodec = Codec.fromEnum(Color.values)

      // Test encoding
      val _ = colorCodec.encode(Color.Red) shouldBe new BsonString("Red")
      val _ = colorCodec.encode(Color.Green) shouldBe new BsonString("Green")
      val _ = colorCodec.encode(Color.Blue) shouldBe new BsonString("Blue")

      // Test decoding
      val _ =
        colorCodec.decode(new BsonString("Red")) shouldBe Success(Color.Red)
      val _ =
        colorCodec.decode(new BsonString("Green")) shouldBe Success(Color.Green)
      val _ =
        colorCodec.decode(new BsonString("Blue")) shouldBe Success(Color.Blue)

      // Test decoding invalid value
      val _ = colorCodec.decode(new BsonString("Yellow")) shouldBe a[Failure[?]]
      val _ = colorCodec.decode(new BsonInt32(1)) shouldBe a[Failure[?]]
    }

    it("should create codec from enum using custom string mapping") {
      val statusMapping = Seq(
        "active" -> Color.Green,
        "error" -> Color.Red,
        "idle" -> Color.Blue
      )
      val statusCodec = Codec.fromEnum(statusMapping)

      // Test encoding with custom mapping
      val _ = statusCodec.encode(Color.Green) shouldBe new BsonString("active")
      val _ = statusCodec.encode(Color.Red) shouldBe new BsonString("error")
      val _ = statusCodec.encode(Color.Blue) shouldBe new BsonString("idle")

      // Test decoding with custom mapping
      val _ = statusCodec.decode(new BsonString("active")) shouldBe Success(
        Color.Green
      )
      val _ =
        statusCodec.decode(new BsonString("error")) shouldBe Success(Color.Red)
      val _ =
        statusCodec.decode(new BsonString("idle")) shouldBe Success(Color.Blue)

      // Test decoding invalid value
      val _ = statusCodec.decode(new BsonString("Red")) shouldBe a[Failure[?]]
      val _ =
        statusCodec.decode(new BsonString("unknown")) shouldBe a[Failure[?]]
    }

    it("should handle roundtrip encoding/decoding for enum values") {
      val colorCodec = Codec.fromEnum(Color.values)

      Color.values.foreach { color =>
        val encoded = colorCodec.encode(color)
        colorCodec.decode(encoded) shouldBe Success(color)
      }
    }
  }
}
