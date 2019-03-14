import Dependencies._
import Settings._
import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}

val commonSettings: sbt.Def.SettingsDefinition = Def.settings(
    scalaVersion_212,
    strictCompile)

val Libraries = new {
  val shared = Def.settings(
    rmgkLogging, scalatags, loci.communication, loci.wsAkka, circe
  )

  val main = shared ++ Def.settings(scalactic,
                                    jsoup,
                                    betterFiles,
                                    decline,
                                    akkaHttp,
                                    scalatest,
                                    scalacheck,
                                    betterFiles,
                                    jsoup)

  val npmDeps = npmDependencies in Compile ++= Seq("mqtt" -> "2.18.2")


  val js: Def.SettingsDefinition = shared ++ Seq(scalajsdom, fontawesome, npmDeps, normalizecss)
}

lazy val server = project.in(file("server"))
                  .settings(
                    name := "server",
                    commonSettings,
                    fork := true,
                    Libraries.main,
                    (Compile / compile) := ((Compile / compile) dependsOn (web / Compile / fastOptJS / webpack)).value,
                    Compile / compile := ((compile in Compile) dependsOn (web / Assets / SassKeys.sassify)).value,
                    (Compile / resources) ++= {
                      val path = (web / Compile / fastOptJS / artifactPath).value.toPath
                      path.getParent.toFile.listFiles().toSeq},
                    (Compile / resources) ++= (web / Assets / SassKeys.sassify).value,
                    )
                  .enablePlugins(JavaServerAppPackaging)
                  .dependsOn(sharedJVM)
                  .dependsOn(rescalaJVM, crdtsJVM)

lazy val web = project.in(file("web"))
               .enablePlugins(ScalaJSPlugin)
               .settings(
                 name := "web",
                 commonSettings,
                 Libraries.js,
                 scalaJSUseMainModuleInitializer := true,
                 webpackBundlingMode := BundlingMode.LibraryOnly(),
                 )
               .dependsOn(sharedJS)
               .enablePlugins(SbtSassify)
               .enablePlugins(ScalaJSBundlerPlugin)
               .dependsOn(rescalatags, crdtsJS)

lazy val shared = crossProject(JSPlatform, JVMPlatform).crossType(CrossType.Pure).in(file("shared"))
                  .settings(
                    name := "shared",
                    commonSettings,
                    Libraries.shared,
                    )
  .jsConfigure(_.dependsOn(crdtsJS))
  .jvmConfigure(_.dependsOn(crdtsJVM))
lazy val sharedJVM = shared.jvm
lazy val sharedJS = shared.js


lazy val rescalatags = ProjectRef(base = file("rescala"), id = "rescalatags")
lazy val rescalaJVM = ProjectRef(base = file("rescala"), id = "rescalaJVM")
lazy val crdtsJVM = ProjectRef(base = file("rescala"), id = "crdtsJVM")
lazy val crdtsJS = ProjectRef(base = file("rescala"), id = "crdtsJS")



