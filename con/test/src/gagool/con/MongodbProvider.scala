package gagool.con

import de.flapdoodle.embed.mongo.commands.ServerAddress
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.TransitionWalker
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait MongodbProvider extends BeforeAndAfterAll { this: Suite =>

  private val log = LoggerFactory.getLogger(getClass)

  def dbName: String

  lazy val mongodb: TransitionWalker.ReachedState[RunningMongodProcess] =
    Mongod.instance().start(Version.Main.V7_0)

  lazy val serverAddress: ServerAddress = mongodb.current().getServerAddress
  lazy val host: String = serverAddress.getHost
  lazy val port: Int = serverAddress.getPort

  override def beforeAll(): Unit =
    log.info(s"Mongodb running on $host:$port") // force start
    super.beforeAll()

  override def afterAll(): Unit = {
    mongodb.close()
    super.afterAll()
  }
}
