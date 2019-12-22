# Apache Storm Word Counter
This is an implementation of a word counter based on Apache Storm. It counts words from some pre-defined sentences which are given in the random order.
The generator emits one sentence per one second. Each word ends up as a message in Console Log with the corresponding number of times it has appeared.

The project is managed by Maven and could be run by the following command:
```
mvn package; mvn exec:java -Dexec.mainClass=com.github.ipergenitsa.apache.storm.wordcount.Main
```
