export MAVEN_REPO=$HOME/.m2/repository

export CLASS_PATH=target/sharedmq-1.0.0.jar
export CLASS_PATH=$CLASS_PATH:$MAVEN_REPO/com/google/guava/guava/19.0/guava-19.0.jar
export CLASS_PATH=$CLASS_PATH:$MAVEN_REPO/org/slf4j/slf4j-api/1.7.20/slf4j-api-1.7.20.jar
export CLASS_PATH=$CLASS_PATH:$MAVEN_REPO/ch/qos/logback/logback-core/1.1.7/logback-core-1.1.7.jar
export CLASS_PATH=$CLASS_PATH:$MAVEN_REPO/ch/qos/logback/logback-classic/1.1.7/logback-classic-1.1.7.jar

java -classpath "$CLASS_PATH" org.sharedmq.QueueTester "$@"

