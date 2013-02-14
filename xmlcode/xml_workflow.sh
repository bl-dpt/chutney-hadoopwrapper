#!/bin/sh
JAR=~/VMSharedFolder/TavernaHadoopWrapper.jar
#hadoop dfs -rmr output TavernaHadoopWrapper-xml*
hadoop jar $JAR -i jisc1-short-list-err.txt -j xml1 -t XML -x ~/VMSharedFolder/xmlcode/migrate.xml
hadoop jar $JAR -i TavernaHadoopWrapper-xml1/part-00000 -j xml2 -t XML -x ~/VMSharedFolder/xmlcode/featureextract.xml
hadoop jar $JAR -i TavernaHadoopWrapper-xml1/part-00000 -j xml3 -t XML -x ~/VMSharedFolder/xmlcode/featureextractjp2.xml
hadoop jar $JAR -i TavernaHadoopWrapper-xml1/part-00000 -j xml4 -t XML -x ~/VMSharedFolder/xmlcode/featurecompare.xml
hadoop jar $JAR -i TavernaHadoopWrapper-xml1/part-00000 -j xml5 -t XML -x ~/VMSharedFolder/xmlcode/jpylyzer.xml
hadoop jar $JAR -i TavernaHadoopWrapper-xml1/part-00000 -j xml6 -t XWR

