# VINF_Information_Retrieval
This repository include code for the subject Information retrieval (FIIT STU)

## Web of our subject
https://vi2022.ui.sav.sk/doku.php?id=start

## How to run docker image
Fow download our image we use this command:

<code>
docker pull iisas/hadoop-spark-pig-hive:2.9.2
</code>

For run our docker image we use this comand:

<code>
docker run -it -p 50070:50070 -p 8088:8088 -p 8080:8080 iisas/hadoop-spark-pig-hive:2.9.2  bash
</code>

To stop or exit Docker image run this command:

<code>
exit
</code>