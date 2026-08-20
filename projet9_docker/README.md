# POC — Pipeline de gestion de tickets clients en streaming

**InduTechData — Projet 9 (Formation OpenClassrooms)**

Proof of Concept démontrant un pipeline de données temps réel simulant l'écosystème
Redpanda + PySpark déployé chez InduTechData suite à leur migration vers AWS.

Les tickets clients sont générés en continu, transmis via Redpanda (compatible Kafka),
puis traités en streaming par PySpark Structured Streaming : enrichissement,
agrégation par type de demande, et export vers des fichiers Parquet pour analyse ultérieure.

## Architecture du pipeline

```mermaid
graph LR
    A[Générateur de tickets] -->|produit JSON| B[(Topic Redpanda<br/>client_tickets)]
    B -->|consomme en streaming| C[PySpark Structured Streaming]
    C -->|enrichissement + jointure| D[df_enrichi]
    C -->|agrégation fenêtrée 24h| E[result_1]
    D -->|export Parquet| F[/data/tickets_enrichis/]
    E -->|export Parquet| G[/data/comptage_par_type/]
```
## Prérequis
 
- Docker et Docker Compose installés
- Ports disponibles sur la machine hôte : `9092`, `8081`, `9644`

 >**Note** : ce dépôt contient aussi des notebooks d'exploration
>(`read_client_tickets.ipynb`, scripts de test Delta/Iceberg, etc.) à la racine.
>Le POC dockerisé, prêt à l'emploi, se trouve dans le sous-dossier `projet9_docker/`.

## Lancement du POC

```bash
git clone https://github.com/ThomaSiesse/Projet_9.git
cd projet9_docker
docker compose up
```

Les 3 services démarrent dans l'ordre : Redpanda (broker), puis le générateur de
tickets et le traitement PySpark une fois Redpanda opérationnel (healthcheck).

## Composants

| Service | Rôle |
|---|---|
| **redpanda** | Broker de streaming (compatible Kafka), reçoit les tickets bruts |
| **ticket-generator** | Génère des tickets clients aléatoires en continu, les envoie au topic `client_tickets` |
| **spark-processing** | Consomme le flux en streaming, enrichit et agrège les tickets, exporte en Parquet |

## Résultats produits

Le traitement PySpark exporte deux jeux de données en Parquet, dans `./data/` :

- **`data/tickets_enrichis/`** — chaque ticket, enrichi avec l'équipe de support
  associée (Comptabilité, SAV, ou Service Client selon le type de demande)
- **`data/comptage_par_type/`** — nombre de tickets par type de demande, agrégé
  sur une fenêtre glissante de 24h

Pour consulter les résultats (une fois des données générées) :

```python
import pandas as pd

df = pd.read_parquet("data/tickets_enrichis")
print(df.head(10))
```

> **Note** : `comptage_par_type` n'apparaît qu'après la clôture d'une fenêtre de
> 24h (+ 1h de watermark). Pour une démo rapide, ces durées sont réduites via les
> variables d'environnement du service `spark-processing` (voir section
> [Configuration](#configuration)).

## Configuration

Le service `spark-processing` peut être ajusté via les variables d'environnement
définies dans `docker-compose.yml` :

| Variable | Valeur par défaut | Rôle |
|---|---|---|
| `WINDOW_DURATION` | `24 hours` | Taille de la fenêtre d'agrégation du comptage par type |
| `WATERMARK_DURATION` | `1 hour` | Délai de tolérance pour les données en retard |
| `TRIGGER_INTERVAL` | `20 seconds` | Fréquence de traitement des micro-batchs |

> Pour une démonstration rapide (voir les résultats sans attendre 24h), réduire
> temporairement `WINDOW_DURATION` à `2 minutes` et `WATERMARK_DURATION` à
> `30 seconds` dans le `docker-compose.yml`, puis relancer avec
> `docker compose up --build`.

## Résilience

Le pipeline intègre plusieurs mécanismes pour limiter l'impact des pannes :

- **Retry au démarrage du générateur** : si Redpanda n'est pas encore prêt à
  accepter des connexions, `ticket-generator` retente automatiquement pendant
  50 secondes avant d'abandonner.
- **Superviseur avec redémarrage automatique** (`spark-processing`) : chaque
  requête de streaming est surveillée ; en cas d'erreur, elle est relancée
  automatiquement (jusqu'à 5 tentatives, avec un délai croissant entre chaque
  essai).
- **Checkpointing persistant** : chaque requête Spark conserve sa progression
  (offsets Kafka traités) dans `./checkpoints/`

## Démonstration vidéo

[![Démonstration du pipeline](https://img.youtube.com/vi/bICGBG1a3Xw/maxresdefault.jpg)](https://youtu.be/bICGBG1a3Xw)

La vidéo présente :
1. Le lancement du pipeline avec `docker compose up`
2. La génération de tickets en direct (logs de `ticket-generator`)
3. Le traitement PySpark en streaming (logs de `spark-processing`)
4. La consultation des résultats exportés (`data/tickets_enrichis/`,
   `data/comptage_par_type/`)