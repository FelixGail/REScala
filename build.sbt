
// set the prompt (for this build) to include the project id.
shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }
// do not spam console with too many errors
maxErrors := 5
crossScalaVersions := Seq(cfg.version_211, cfg.version_212)
(incOptions in ThisBuild) := (incOptions in ThisBuild).value.withLogRecompileOnMacro(false)


lazy val rescalaAggregate = project.in(file(".")).aggregate(rescalaJVM,
  rescalaJS, microbench, reswing, examples, examplesReswing, caseStudyEditor,
  caseStudyRSSEvents, caseStudyRSSReactive, caseStudyRSSSimple, rescalatags,
  datastructures, universe, reactiveStreams, documentation,
  stm, testToolsJVM, testToolsJS, testsJVM, testsJS, fullmv, caseStudyShapes, caseStudyMill)
  .settings(cfg.noPublish)


lazy val rescala = crossProject.in(file("Main"))
  .settings(cfg.base)
  .settings(name := "rescala",
    cfg.strictScalac, cfg.snapshotAssertions, lib.circe,
    cfg.generateLiftFunctions, lib.sourcecode)
  .settings(lib.retypecheck)
  .settings(cfg.bintray)
  .jvmSettings()
  .jsSettings(cfg.js)
//  .nativeSettings(
//    crossScalaVersions := Seq("2.11.8"),
//    scalaVersion := "2.11.8")

lazy val rescalaJVM = rescala.jvm

lazy val rescalaJS = rescala.js

//lazy val rescalaNative = rescala.native

lazy val testTools = crossProject.in(file("TestTools"))
  .settings(cfg.base)
  .settings(name := "rescala-testtoolss", cfg.noPublish)
  .settings(cfg.test)
  .dependsOn(rescala)
  .jvmSettings().jsSettings(cfg.js)
lazy val testToolsJVM = testTools.jvm
lazy val testToolsJS = testTools.js

lazy val tests = crossProject.in(file("Tests"))
  .settings(cfg.base)
  .settings(name := "rescala-tests", cfg.noPublish)
  .settings(cfg.test)
  .dependsOn(rescala)
  .jvmSettings().jsSettings(cfg.js)
lazy val testsJVM = tests.jvm.dependsOn(testToolsJVM % "test->test", fullmv, stm)
lazy val testsJS = tests.js.dependsOn(testToolsJS % "test->test")

lazy val documentation = project.in(file("Documentation/DocumentationProject"))
  .settings(cfg.base)
  .enablePlugins(TutPlugin)
  .dependsOn(rescalaJVM, rescalaJS)
  .settings(cfg.noPublish)


// Extensions

lazy val reactiveStreams = project.in(file("Extensions/ReactiveStreams"))
  .settings(cfg.base)
  .dependsOn(rescalaJVM)
  .settings(lib.reactivestreams)
  .settings(cfg.noPublish)

lazy val reandroidthings = project.in(file("Extensions/REAndroidThings"))
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .enablePlugins(AndroidLib)
  .dependsOn(rescalaJVM)
  .settings(
    name := "reandroidthings",
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"))

lazy val reswing = project.in(file("Extensions/RESwing"))
  .settings(cfg.base)
  .settings(cfg.strictScalac)
  .dependsOn(rescalaJVM)
  .settings(cfg.bintray)
  .settings(lib.scalaswing)
  .settings(name := "reswing")

lazy val rescalatags = project.in(file("Extensions/Rescalatags"))
  .settings(cfg.base)
  .settings(cfg.strictScalac)
  .enablePlugins(ScalaJSPlugin)
  .dependsOn(rescalaJS)
  .settings(cfg.js, lib.scalatags, jsDependencies += RuntimeDOM)
  .settings(cfg.bintray)
  .settings(cfg.test)

lazy val datastructures = project.in(file("Extensions/Datastructures"))
  .dependsOn(rescalaJVM)
  .settings(cfg.base)
  .settings(name := "datastructures")
  .settings(lib.scalatest)
  .settings(cfg.noPublish)

lazy val stm = project.in(file("Extensions/STM"))
  .settings(cfg.base)
  .dependsOn(rescalaJVM)
  .settings(cfg.noPublish)
  .settings(lib.scalaStm)

// Examples

lazy val examples = project.in(file("Examples/examples"))
  .dependsOn(rescalaJVM)
  .settings(cfg.base)
  .settings(name := "rescala-examples")
  .settings(cfg.noPublish)
  .settings(lib.scalaswing)

lazy val examplesReswing = project.in(file("Examples/examples-reswing"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(name := "reswing-examples")
  .settings(cfg.noPublish)


lazy val baromter4Android = project.in(file("Examples/Barometer4Android"))
  .enablePlugins(AndroidApp)
  .dependsOn(reandroidthings)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(
    name := "barometer4Android",
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
    lib.android,
    platformTarget := "android-25", //TODO: Move to androidJVM
    android.useSupportVectors,
    instrumentTestRunner := "android.support.test.runner.AndroidJUnitRunner")

lazy val caseStudyEditor = project.in(file("Examples/Editor"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(name := "editor-case-study")

lazy val caseStudyRSSEvents = project.in(file("Examples/RSSReader/ReactiveScalaReader.Events"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(name := "rssreader-case-study", lib.rss)
  .settings(cfg.noPublish)
  .settings(cfg.test)

lazy val caseStudyRSSReactive = project.in(file("Examples/RSSReader/ReactiveScalaReader.Reactive"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(name := "rssreader-case-study-reactive", lib.rss)
  .settings(cfg.noPublish)
  .settings(cfg.test)

lazy val caseStudyRSSSimple = project.in(file("Examples/RSSReader/SimpleRssReader"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(name := "rssreader-case-study-simple", lib.rss)
  .settings(cfg.noPublish)
  .settings(cfg.test)

lazy val universe = project.in(file("Examples/Universe"))
  .dependsOn(rescalaJVM)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(name := "rescala-universe")
  .settings(com.typesafe.sbt.SbtStartScript.startScriptForClassesSettings)

lazy val caseStudyShapes = project.in(file("Examples/Shapes"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(name := "shapes-case-study", lib.scalaXml)

lazy val caseStudyMill = project.in(file("Examples/Mill"))
  .dependsOn(reswing)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(name := "mill-case-study")


// Research

lazy val fullmv = project.in(file("Research/Multiversion"))
  .settings(cfg.base)
  .settings(name := "rescala-multiversion")
  .settings(cfg.test)
  .settings(cfg.noPublish)
  .dependsOn(rescalaJVM, testToolsJVM % "test->test")

lazy val meta = project.in(file("Research/Meta"))
  .dependsOn(rescalaJVM)
  .settings(cfg.test)
  .settings(cfg.noPublish)

lazy val microbench = project.in(file("Research/Microbenchmarks"))
  .enablePlugins(JmhPlugin)
  .settings(cfg.base)
  .settings(cfg.noPublish)
  .settings(mainClass in Compile := Some("org.openjdk.jmh.Main"))
  .settings(com.typesafe.sbt.SbtStartScript.startScriptForClassesSettings)
  .settings(TaskKey[Unit]("compileJmh") := Seq(compile in pl.project13.scala.sbt.SbtJmh.JmhKeys.Jmh).dependOn.value)
  .dependsOn(stm, fullmv)


// ================================== settings

lazy val cfg = new {

  val version_211 = "2.11.11"
  val version_212 = "2.12.2"


  val base = List(
    organization := "de.tuda.stg",
    version := "0.20.0-SNAPSHOT",
    scalaVersion := version_212,
    baseScalac
  )

  val test = List(
    testOptions in Test += Tests.Argument("-oICN"),
    parallelExecution in Test := true,
    lib.scalatest
  )

  val bintray = List(
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    bintrayOrganization := Some("stg-tud")
  )

  val noPublish = List(
    publish := {},
    publishLocal := {}
  )

  val js = scalaJSUseRhino in Global := true

  lazy val baseScalac = scalacOptions ++= List(
    "-deprecation",
    "-encoding", "UTF-8",
    "-unchecked",
    "-feature",
    "-Xlint",
    "-Xfuture"
  )

  lazy val strictScalac = scalacOptions ++= List(
    //"-Xlog-implicits" ,
    //"-Yno-predef" ,
    //"-Yno-imports" ,
    "-Xfatal-warnings",
    //"-Yinline-warnings" ,
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen"
    //"-Ywarn-value-discard" ,
    //"-Ymacro-debug-lite" ,
  )

  lazy val snapshotAssertions = scalacOptions ++= (if (!version.value.endsWith("-SNAPSHOT")) List("-Xdisable-assertions", "-Xelide-below", "9999999")
  else Nil)


  val generateLiftFunctions = sourceGenerators in Compile += Def.task {
    val file = (sourceManaged in Compile).value / "rescala" / "reactives" / "GeneratedSignalLift.scala"
    val definitions = (1 to 22).map { i =>
      val params = 1 to i map ("n" + _)
      val types = 1 to i map ("A" + _)
      val signals = params zip types map { case (p, t) => s"$p: Signal[$t, S]" }
      def sep(l: Seq[String]) = l.mkString(", ")
      val getValues = params map (v => s"t.staticDepend($v).get")
      s"""  def lift[${sep(types)}, B, S <: Struct](${sep(signals)})(fun: (${sep(types)}) => B)(implicit maybe: CreationTicket[S]): Signal[B, S] = {
         |    static(${sep(params)})(t => fun(${sep(getValues)}))
         |  }
         |""".stripMargin
    }
    IO.write(file,
      s"""package rescala.reactives
         |
         |import rescala.core._
         |
         |trait GeneratedSignalLift {
         |self: Signals.type =>
         |${definitions.mkString("\n")}
         |}
         |""".stripMargin)
    Seq(file)
  }.taskValue

}

// ================================ dependencies

lazy val lib = new {

  lazy val android = libraryDependencies ++= Seq(
    "com.android.support" % "appcompat-v7" % "25.3.1",
    "com.android.support.test" % "runner" % "0.5" % "androidTest",
    "com.android.support.test.espresso" % "espresso-core" % "2.2.2" % "androidTest",
    scalaOrganization.value % "scala-reflect" % scalaVersion.value)

  lazy val rss = libraryDependencies ++= Seq(
    "joda-time" % "joda-time" % "2.9.9",
    "org.joda" % "joda-convert" % "1.8.1",
    "org.codehaus.jsr166-mirror" % "jsr166y" % "1.7.0",
    "org.scala-lang.modules" %% "scala-xml" % "1.0.6")

  lazy val scalaswing = libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "2.0.0"
  lazy val scalatest = libraryDependencies += "org.scalatest" %%% "scalatest" % "3.0.3" % "test"


  lazy val circe = {
    libraryDependencies ++= Seq(
      "io.circe" %%% "circe-core",
      "io.circe" %%% "circe-generic",
      "io.circe" %%% "circe-parser"
    ).map(_ % "0.8.0")
  }

  val reactivestreams = libraryDependencies ++= List(
    "org.reactivestreams" % "reactive-streams" % "1.0.0",
    "org.reactivestreams" % "reactive-streams-tck" % "1.0.0"
  )

  val scalaStm = libraryDependencies += "org.scala-stm" %% "scala-stm" % "0.8"

  val retypecheck = List(
    resolvers += Resolver.bintrayRepo("pweisenburger", "maven"),
    libraryDependencies += "de.tuda.stg" %% "retypecheck" % "0.3.0"
  )

  val reflectionForMacroDefinitions = libraryDependencies += scalaOrganization.value % "scala-reflect" % scalaVersion.value % "provided"

  val sourcecode = libraryDependencies += "com.lihaoyi" %%% "sourcecode" % "0.1.4"

  val scalaXml = libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.6"

  val scalatags = libraryDependencies += "com.lihaoyi" %%% "scalatags" % "0.6.5"

}
