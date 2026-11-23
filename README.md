# Projet 9 : modéliser une infrastructure dans le cloud

Conception d'une infrastructure hybride **on-premise / AWS** pour InduTechData, et POC d'un pipeline de données en temps réel avec **Redpanda** et **PySpark Structured Streaming**, entièrement conteneurisé avec Docker.

## Contexte

**InduTechData** exploite un système d'information on-premise :

- un cluster **SQL Server** (40 To de données ERP/CRM) ;
- une baie **SAN** (10 To de données non structurées : logs, fichiers, capteurs IoT) ;
- un annuaire **Active Directory**.

L'entreprise veut absorber des flux temps réel et construire un data lake analytique sur AWS, **sans remplacer le SI existant**. La mission comporte trois volets :

1. **Modéliser** l'architecture hybride cible (on-premise + AWS + Redpanda) ;
2. **Évaluer** sa compatibilité avec l'existant : sécurité, interopérabilité, scalabilité, coûts ;
3. **Démontrer** la faisabilité avec un POC de traitement en streaming de tickets clients.

## Livrables

Ils se trouvent dans le dossier `Modelisez_une_infrastructure_dans_le_cloud_siesse_thomas/`.

| Fichier                                    | Contenu                                                                                                |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `Siesse_Thomas_1_schema_072026`          | Schéma de l'architecture hybride (draw.io, à ouvrir sur [app.diagrams.net](https://app.diagrams.net)) |
| `Siesse_Thomas_2_evaluation_072026.docx` | Évaluation de compatibilité de l'infrastructure                                                      |
| `Siesse_Thomas_3_code_072026.zip`        | Code du POC                                                                                            |
| `Siesse_Thomas_4_schema_flux_072026.png` | Schéma des flux du pipeline                                                                           |
| `Siesse_Thomas_5_demo_072026.mp4`        | Vidéo de démonstration                                                                               |

## Architecture hybride

| Côté     | Composants                                                                                                              |
| ---------- | ----------------------------------------------------------------------------------------------------------------------- |
| On-premise | SQL Server (ERP/CRM), baie SAN (fichiers, IoT), Active Directory, applications métier                                  |
| Streaming  | Redpanda sur EC2 (compatible Kafka) : ingestion des flux temps réel                                                    |
| AWS        | S3 (data lake), Redshift (entrepôt, schéma en étoile), Lake Formation (gouvernance), CloudWatch (logs et métriques) |

### Points clés de l'évaluation

- **Sécurité** :
  - TLS est natif sur les services AWS, mais doit être activé explicitement sur Redpanda ;
  - Active Directory et IAM ne sont pas unifiés : il faut une fédération, par AD Connector ou IAM Identity Center.
- **Interopérabilité** :
  - l'API Kafka de Redpanda donne accès aux connecteurs Kafka Connect ;
  - Debezium permet la capture de changements (CDC) depuis SQL Server, et un connecteur JDBC permet le retour vers SQL Server.
- **Scalabilité** :
  - côté Redpanda, on peut ajouter des partitions et des brokers ;
  - S3 offre un stockage élastique, et Redshift Serverless ajuste sa capacité à la charge.
- **Coûts** :
  - environ 200 à 350 $ par mois pour un pilote ;
  - suivi avec AWS Budgets, des alarmes CloudWatch, Cost Explorer et un tagging des ressources.

## POC : pipeline de tickets clients en streaming

```text
ticket-generator ──JSON──▶ Redpanda (topic client_tickets) ──▶ PySpark Structured Streaming
                                                                   ├─ enrichissement ─▶ data/tickets_enrichis/   (Parquet)
                                                                   └─ comptage fenêtré ▶ data/comptage_par_type/ (Parquet)
```

| Service              | Rôle                                                                                        |
| -------------------- | -------------------------------------------------------------------------------------------- |
| `redpanda`         | Broker de streaming compatible Kafka (v25.2.1)                                               |
| `ticket-generator` | Génère un ticket par seconde : id, client, date, demande, type, priorité pondérée       |
| `spark-processing` | Lit le topic, enrichit chaque ticket et agrège par type de demande, puis exporte en Parquet |

Le traitement suit une logique en couches :

- **Bronze** : les tickets bruts arrivent dans Redpanda.
- **Silver** : chaque ticket est enrichi avec l'équipe de support associée, par une jointure broadcast :
  - Facturation → Comptabilité ;
  - Support technique → SAV ;
  - les autres types → Service Client.
- **Gold** : les tickets sont comptés par type de demande sur une fenêtre temporelle, avec un watermark pour gérer les données en retard.

Mécanismes de résilience :

- checkpoints persistants (offsets traités) ;
- superviseur qui relance chaque requête de streaming jusqu'à 5 fois, avec un délai croissant ;
- `restart: unless-stopped` sur les conteneurs ;
- healthcheck sur Redpanda avant le démarrage des autres services.

### Lancement

Prérequis : Docker et Docker Compose. Les ports `9092`, `8081` et `9644` doivent être libres.

```bash
cd projet9_docker
docker compose up --build
```

Pour lire les résultats :

```python
import pandas as pd
pd.read_parquet("projet9_docker/data/tickets_enrichis").head()
```

| Variable (`spark-processing`) | Défaut dans le code | Valeur dans `docker-compose.yml` |
| ------------------------------- | -------------------- | ---------------------------------- |
| `WINDOW_DURATION`             | `24 hours`         | `2 minutes` (démo)              |
| `WATERMARK_DURATION`          | `1 hour`           | `30 seconds` (démo)             |
| `TRIGGER_INTERVAL`            | `20 seconds`       | `20 seconds`                     |

Le détail du POC est documenté dans [projet9_docker/README.md](projet9_docker/README.md). Une vidéo de démonstration est disponible sur [YouTube](https://youtu.be/bICGBG1a3Xw).

## Structure du dépôt

| Élément                                                                                                                                            | Description                                                               |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `projet9_docker/`                                                                                                                                  | POC dockerisé, prêt à l'emploi (generator, redpanda, spark_processing) |
| `Siesse_Thomas_3_code_072026/`                                                                                                                     | Copie du code du POC remise en livrable                                   |
| `ticket_generator.py`, `read_client_tickets.py` / `.ipynb`                                                                                     | Versions de développement du générateur et du traitement Spark         |
| `cours_pyspark.*`, `greenfarm.*`, `testdelta.py`, `testiceberg.py`, `miniotest*.py`, `processraw.py`, `iotprocess.py`, `newtable.py` | Exercices du cours : PySpark, Delta Lake, Iceberg, MinIO                  |
| `data/`, `delta_demo/`, `iceberg_demo/`, `table_greefarm*/`, `checkpoints/`                                                                | Données produites par les exercices et le développement                 |
| `Taches.rm`                                                                                                                                        | Journal de bord du projet                                                 |

## Stack technique

Python, PySpark 4.2, Spark Structured Streaming, Redpanda (Kafka), kafka-python, Parquet, Docker, Docker Compose, AWS (S3, Redshift, Lake Formation, EC2, CloudWatch, IAM), draw.io.

## Auteur

Thomas SIESSE, août 2026.
