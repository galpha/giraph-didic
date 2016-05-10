## Distributed Diffusive Clustering mit Apache Giraph

In diesem Projekt wurder der Distributed Diffusive Clustering Algorithmus (kurz [DiDiC](http://parco.iti.kit.edu/henningm/data/distribClust.pdf))
auf Apache Giraph (1.1.0) umgesetzt.

#### Zusammenfassung

Graphen werden in vielen Domänen eingesetzt, um Datenstrukturen zu modellieren: in der Biologie, in der Business Intelligence, in sozialen Netzwerken und vielen anderen.
Ein häufig auftretendes Problem ist das Finden von Teilgraphen, deren Elemente besonders stark verknüpft sind, sogenannte Cluster. Zusätzlich stellt die immer weiter wachsende Größe der Graphen
Anforderungen an die Parallelisierbarkeit von Algorithmen, die auf Graphen arbeiten. Im Rahmen unserer Arbeit haben wir den Clustering Algorithmus DiDiC auf dem Graph-Processing Framework Apache Giraph implementiert.

#### Verwendung

Für die Verwendung benötigt man Maven 2 (oder neuere Version).


* Clone giraph-didic auf das lokale File-System

    > git clone https://github.com/galpha/giraph-didic.git

* Bauen und Tests ausführen

    > cd giraph-didic

    > mvn clean package

* Auf laufenden Hadoop-Cluster ausführen

    > $HADOOP_PREFIX/bin/hadoop jar giraph_didic-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner diffusionclustering.DiffusionComputation -mc diffusionclustering.DiffusionMasterComputation -vif diffusionclustering.DiffusionTextVertexInputFormat -vip /user/input/as-skitter.txt.out -vof diffusionclustering.DiffusionTextVertexOutputFormat -op /user/output -ca diffusion.cluster.num=4 -w 32

#### Beispiel (Ergebnisse)

Am Beispiel des Facebook-Datensatz (von [SnapStanford](http://snap.stanford.edu/data/index.html)) wurderder Algorithmus ausgeführt um 10 Cluster zu identifizieren.

<p align="center">
  <img src="https://img1.picload.org/image/rgoadrll/graph.png" alt="Facebook 10 Cluster"/>
</p>






