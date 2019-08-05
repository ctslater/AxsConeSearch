name := "axs-vocone"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0" // % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0" // % "provided"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.5"
libraryDependencies += "uk.ac.starlink" % "stil-io" % "3.3.2"
libraryDependencies += "gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.15.2"


// Required for stil-io
resolvers += Resolver.jcenterRepo

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

