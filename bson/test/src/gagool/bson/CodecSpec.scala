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
      encodedDog shouldBe new BsonString("Dog: Buddy")

      // Thanks to contravariance, an Encoder[Animal, BsonValue] can also be assigned to a variable of type Encoder[Dog, BsonValue] directly
      val dogEncoderFromAnimalDirect: Encoder[Dog, BsonValue] =
        animalNameEncoder
      dogEncoderFromAnimalDirect.encode(dog) shouldBe new BsonString(
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
      stringDecoder.decode(new BsonString("hello")) shouldBe Success("hello")
      stringDecoder.decode(new BsonInt32(123)) shouldBe a[Failure[?]]
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
        if (i > 0) Success(i)
        else Failure(new IllegalArgumentException("Negative int"))
      }
      positiveIntDecoder.decode(new BsonInt32(5)) shouldBe Success(5)
      positiveIntDecoder.decode(new BsonInt32(-5)) shouldBe a[Failure[?]]
    }

    it("should support orElse for fallback decoding") {
      val intDecoder: BsonValueDecoder[Int] = BaseCodecs.intCodec
      val fallbackDecoder: BsonValueDecoder[Int] = intDecoder.orElse(Success(0))

      fallbackDecoder.decode(new BsonInt32(5)) shouldBe Success(5)
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
      animalDecoder.decode(new BsonString("Dog: Fido")) shouldBe Success(
        Dog("Fido")
      )
      animalDecoder
        .decode(new BsonString("Cat: Whiskers")) shouldBe a[Failure[?]]
    }

    it("should support contravariance for input type") {

      val stringBsonValueDecoder = BaseCodecs.stringCodec
      stringBsonValueDecoder.decode(new BsonString("test")) shouldBe Success(
        "test"
      )
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

      customCodec.encode(5) shouldBe new BsonInt32(10)
      customCodec.decode(new BsonInt32(10)) shouldBe Success(5)
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
      wrapperCodec.encode(wrapper) shouldBe new BsonString("WrappedText")
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
      encoded.getValues.asScala
        .map(_.asString().getValue)
        .toSet shouldBe originalSet
      setStringCodec.decode(encoded) shouldBe Success(originalSet)
    }

    it("should encode and decode Option correctly") {
      val optionIntCodec = BaseCodecs.optionCodec[Int]
      optionIntCodec.encode(Some(123)) shouldBe new BsonInt32(123)
      optionIntCodec.encode(None) shouldBe BsonNull.VALUE
      optionIntCodec.decode(new BsonInt32(123)) shouldBe Success(Some(123))
      optionIntCodec.decode(BsonNull.VALUE) shouldBe Success(None)
    }

    it("should encode and decode List correctly") {
      val listIntCodec = BaseCodecs.listCodec[Int]
      val originalList = List(1, 2, 3)
      val encoded = listIntCodec.encode(originalList).asInstanceOf[BsonArray]
      encoded.getValues.asScala
        .map(_.asInt32().getValue)
        .toList shouldBe originalList
      listIntCodec.decode(encoded) shouldBe Success(originalList)
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
      decodedMap shouldBe originalMap
      mapStringCodec.decode(encoded) shouldBe Success(originalMap)
    }

    it("should handle mixed numeric types for intCodec") {
      val intCodec = BaseCodecs.intCodec
      intCodec.decode(new BsonInt32(10)) shouldBe Success(10)
      intCodec.decode(new BsonInt64(10L)) shouldBe Success(
        10
      ) // truncation is fine for this test
      intCodec.decode(new BsonDouble(10.5)) shouldBe Success(
        10
      ) // truncation is fine for this test
      intCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for longCodec") {
      val longCodec = BaseCodecs.longCodec
      longCodec.decode(new BsonInt64(100L)) shouldBe Success(100L)
      longCodec.decode(new BsonInt32(100)) shouldBe Success(100L)
      longCodec.decode(new BsonDouble(100.5)) shouldBe Success(100L)
      longCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for floatCodec") {
      val floatCodec = BaseCodecs.floatCodec
      floatCodec.decode(new BsonDouble(10.5f)) shouldBe Success(10.5f)
      floatCodec.decode(new BsonInt32(10)) shouldBe Success(10.0f)
      floatCodec.decode(new BsonInt64(10L)) shouldBe Success(10.0f)
      floatCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle mixed numeric types for doubleCodec") {
      val doubleCodec = BaseCodecs.doubleCodec
      doubleCodec.decode(new BsonDouble(10.5)) shouldBe Success(10.5)
      doubleCodec.decode(new BsonInt32(10)) shouldBe Success(10.0)
      doubleCodec.decode(new BsonInt64(10L)) shouldBe Success(10.0)
      doubleCodec.decode(new BsonString("hello")) shouldBe a[Failure[?]]
    }

    it("should handle BsonNull for stringCodec") {
      val stringCodec = BaseCodecs.stringCodec
      stringCodec.decode(BsonNull.VALUE) shouldBe a[Failure[?]]
    }

    it("should handle BsonBoolean for boolCodec") {
      val boolCodec = BaseCodecs.boolCodec
      boolCodec.encode(true) shouldBe new BsonBoolean(true)
      boolCodec.decode(new BsonBoolean(false)) shouldBe Success(false)
      boolCodec.decode(new BsonInt32(1)) shouldBe a[Failure[?]]
    }
  }
}
