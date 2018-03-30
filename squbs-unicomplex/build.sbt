import Versions._

name := "squbs-unicomplex"

javaOptions in Test += "-Xmx512m"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalatest" %% "scalatest" % scalatestV % "test->*",
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
  "ch.qos.logback" % "logback-classic" % logbackInTestV % "test",
  "com.wix" %% "accord-core" % accordV % "test",
  "junit" % "junit" % junitV % "test",
  "com.novocode" % "junit-interface" % junitInterfaceV % "test->default",
  "org.scalatest" %% "scalatest" % scalatestV % "test->*"
) ++ akka ++ proteus

def akka = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-agent" % akkaV,
  "com.typesafe.akka" %% "akka-http" % akkaHttpV,
  "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test"
)

def proteus = Seq(
  "io.netifi.proteus" % "core" % proteusV,
  "io.netifi.proteus" % "client" % proteusV,
  "io.projectreactor" %% "reactor-scala-extensions" % reactorScalaExtensionsV
)

testOptions in Test ++= Seq(
  Tests.Argument(TestFrameworks.ScalaTest, "-h", "report/squbs-unicomplex"),
  Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
)


updateOptions := updateOptions.value.withCachedResolution(true)