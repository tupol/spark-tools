import Dependencies._
import sbt.Keys.{fork, resolvers}
import sbt.{Developer, ScmInfo, url}
import sbtassembly.AssemblyPlugin.defaultShellScript
import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, commitNextVersion, commitReleaseVersion, inquireVersions, publishArtifacts, pushChanges, runClean, runTest, setNextVersion, setReleaseVersion, tagRelease}

lazy val basicSettings = Seq(
  organization := "org.tupol",
  name := "spark-tools",
  scalaVersion := Versions.scala,
  crossScalaVersions := Versions.crossScala,
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-Ywarn-unused-import",
    s"-target:jvm-${Versions.java}"
  ),
  javacOptions ++= Seq("-source", Versions.java, "-target", Versions.java),
  updateOptions := updateOptions.value.withCachedResolution(true),
  libraryDependencies ++= TestDependencies,
  dependencyOverrides ++= FasterXmlOverrides,
  resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  parallelExecution in Test := false,
  fork in Test := true
)


lazy val publishSettings = Seq(
  isSnapshot := version.value.trim.endsWith("SNAPSHOT"),
  // Nexus (see https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html)
  publishTo := {
    val repo = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at repo + "content/repositories/snapshots")
    else Some("releases" at repo + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := true,
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  licenses := Seq("MIT-style" -> url("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/tupol/spark-tools")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/tupol/spark-tools.git"),
      "scm:git@github.com:tupol/spark-tools.git"
    )
  ),
  developers := List(
    Developer(
      id = "tupol",
      name = "Tupol",
      email = "tupol.github@gmail.com",
      url = url("https://github.com/tupol")
    )
  ),
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,          // performs the initial git checks
    tagRelease,
    publishArtifacts,              // checks whether `publishTo` is properly set up
    releaseStepCommand(s"""sonatypeOpen "${organization.value}" "${name.value} v${version.value}""""),
    releaseStepCommand("publishSigned"),
    releaseStepCommand("sonatypeRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges                     // also checks that an upstream branch is properly configured
  )
)

lazy val coverageSettings = Seq(
  coverageEnabled in Test := true,
  coverageMinimumStmtTotal in Test := 90,
  coverageFailOnMinimum in Test := true,
  coverageExcludedPackages := "org.apache.spark.ml.param.shared.*;.*BuildInfo.*;org.tupol.spark.Logging.*"
)

lazy val assemblySettings =
  Seq(
    packageBin / assembleArtifact := true,
    assemblyPackageScala / assembleArtifact := true,
    assemblyPackageDependency / assembleArtifact := true,
    assembly / assemblyOption := (assembly / assemblyOption).value
      .copy(prependShellScript = Some(defaultShellScript)),
    assembly / assemblyMergeStrategy := {
      case "module-info.class"                               => MergeStrategy.discard
      case x if x.endsWith("/module-info.class")             => MergeStrategy.discard
      case x if x.contains("scala/annotation/nowarn.class")  => MergeStrategy.first
      case x if x.endsWith("scala/annotation/nowarn$.class") => MergeStrategy.first
      case PathList("META-INF", _)                           => MergeStrategy.discard
      case x                                                 =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

val commonSettings = basicSettings ++ coverageSettings ++ publishSettings

lazy val root = Project(
  id = "spark-tools",
  base = file(".")
)
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-tools",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.tools.info",
    libraryDependencies ++= Dependencies.MainDependencies,
    libraryDependencies ++= Dependencies.ProvidedSparkDependencies,
    libraryDependencies ++= Dependencies.TestDependencies,
    assemblySettings,
    assembly / assemblyJarName := s"${name.value}-pkg-${version.value}.jar",
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
      ShadeRule.rename("com.typesafe.config.**" -> "typesafe_config.@1").inAll
    ),
    addArtifact(assembly / artifact, assembly),
  )
