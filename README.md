
# 🚀 TP Big Data : Pipeline E-Commerce Temps Réel

> **De l'interaction utilisateur au Data Lake : Kafka, Spark & Google Cloud**

## 📝 Présentation du Projet

L'objectif de ce projet était de mettre en place une architecture **End-to-End** capable de capturer les événements d'un site e-commerce (clics, ajouts au panier, vues de produits) et de les traiter en temps réel.

On a simulé un environnement de production en séparant bien les rôles :

1. **Le Site Web** (Backend Node.js) qui génère la donnée.
2. **Kafka** qui sert de "tampon" (Ingestion).
3. **Spark Dataproc** qui fait le calcul lourd (Processing).
4. **Cloud Storage** qui sert de "Data Lake" (Stockage final).

---

## 🛠️ 1. Infrastructure et Réseau (Le "Cerveau")

C’est là qu’on a passé le plus de temps. On a utilisé **Google Cloud Platform (GCP)**.

### Configuration des Instances

* **VM Kafka :** Une instance Compute Engine simple (Debian).
* 
**IP Publique :** `34.78.144.120` (Pour que le site web puisse envoyer les données). 


* **Port :** `9092` ouvert dans le pare-feu GCP.


* 
**Cluster Dataproc (Spark) :** * **Config finale :** 1 Master (`n1-standard-4` - 15 Go RAM) + 2 Workers (`n1-standard-2`). 


* 
**Pourquoi ?** On a dû augmenter la RAM du Master car JupyterLab et Spark ensemble faisaient planter les petites machines. 





---

## 📥 2. Couche d'Ingestion : Kafka

On a installé Kafka manuellement sur la VM.

**Commandes clés sur la VM :**

```bash
# Lancer Kafka (Mode KRaft)
bin/kafka-server-start.sh -daemon config/kraft/server.properties

# Créer le topic pour le site
bin/kafka-topics.sh --create --topic web-events --bootstrap-server localhost:9092

# Tester si on reçoit bien les JSON du site
bin/kafka-console-consumer.sh --topic web-events --bootstrap-server localhost:9092 --from-beginning

```

---

## ⚡ 3. Couche de Traitement : PySpark

C'est le cœur du projet. Spark lit le flux Kafka en continu.

### Le gros défi : Le connecteur Kafka

Au début, Spark ne reconnaissait pas le format "kafka". On a résolu ça en forçant le téléchargement du package au lancement. 

**Lancer le script sur le cluster :**

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master yarn \
  --deploy-mode client \
  spark_analytics.py

```

### Le Code (Logique)
'''python
  from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder \
    .appName("EcommerceRealTimeAnalytics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 1. Schéma JSON
schema = StructType([
    StructField("type", StringType(), True),
    StructField("guestId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("url", StringType(), True),
    StructField("payload", StringType(), True)
])

kafka_ip = "10.132.0.3" 

# 2. Lecture du flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_ip}:9092") \
    .option("subscribe", "web-events") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Parsing et typage du timestamp
events = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# --- CONFIGURATION DU DATA LAKE (GCS) ---
# def  bucket créé sur GCP
bucket_name = "tp-bigdata-datalake"
checkpoint_path = f"gs://{bucket_name}/checkpoints/web_events"
storage_path = f"gs://{bucket_name}/data/web_events"

# 4. Écriture dans le Data Lake au format Parquet
# Le checkpoint est OBLIGATOIRE pour le streaming (évite de perdre le fil si crash)
query_storage = events.writeStream \
    .format("parquet") \
    .option("path", storage_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("type") \
    .start()

# 5. Affichage console 
query_console = events.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print(f">>> Analyseur Spark actif. Stockage vers gs://{bucket_name}...")

# Attente des deux flux
spark.streams.awaitAnyTermination()
'''
On définit un **schéma** pour transformer le JSON brut en tableau propre, puis on écrit dans le Data Lake. 

---

## 🗄️ 4. Stockage Final : Data Lake (GCS)

Les données arrivent dans notre Bucket GCS au format **Parquet**.

* **Pourquoi Parquet ?** C'est un format compressé et "colonnaire". C'est beaucoup plus rapide que le CSV pour faire des stats plus tard. 


* 
**Organisation :** On a partitionné par `type` d'événement (ex: un dossier pour les clics, un pour les achats). 



---

## ⚠️ Problèmes rencontrés & Solutions

| Problème | Ce qu'on a fait |
| --- | --- |
| **AnalysisException: Failed to find data source: kafka** | C'était le driver manquant. On l'a ajouté avec `--packages` dans la commande `spark-submit`. 

 |
| **Job not accepted / RAM saturée** | Jupyter consommait toute la RAM. On a éteint les kernels et recréé un cluster plus puissant (`n1-standard-4`). 

 |
| **Timeout Connexion Kafka** | Le site web n'arrivait pas à joindre la VM. On a dû configurer les `advertised.listeners` avec l'IP publique dans Kafka. 

 |

---

## 📊 Résultat Final

[Image d'un schéma simplifié montrant le flux : Site Web -> Kafka (IP Publique) -> Spark (IP Interne) -> GCS Parquet]

Une fois que tout tourne :

1. Tu cliques sur un produit sur le site.
2. Le log apparaît instantanément dans la console Kafka.
3. Spark récupère la ligne, l'analyse et l'écrit dans le Bucket.
4. Dans Cloud Storage, un nouveau fichier `.parquet` apparaît !

---

## 👨‍💻 Comment relancer le projet ?

1. Démarrer la VM Kafka et lancer le serveur.
2. S'assurer que le backend pointe sur la bonne IP Kafka.
3. Sur Dataproc, uploader `spark_analytics.py` et lancer le `spark-submit`.
4. Ouvrir le Bucket GCS pour admirer les fichiers.

