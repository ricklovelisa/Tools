mainClass in (Compile, packageBin) := Some("tools.HDFStest")

name := "tools"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Kunyan Repo" at "http://61.147.80.245:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.8.3"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies +="org.apache.hbase" % "hbase-server" % "1.1.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

assemblyMergeStrategy in assembly := {
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("org", "objectweb", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

