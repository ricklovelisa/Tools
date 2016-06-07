name := "tools"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.6.6"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"
