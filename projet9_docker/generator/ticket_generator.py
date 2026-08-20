"""
Producteur de tickets clients vers Redpanda (topic: client_tickets)
POC InduTech - Projet 9

Génère des tickets aléatoires en continu et les envoie dans le topic Kafka/Redpanda
"client_tickets", au format JSON. Pensé pour être consommé ensuite via
PySpark Structured Streaming (spark.readStream).

Installation requise :
    pip install kafka-python

Usage :
    python ticket_generator.py
    python ticket_generator.py --interval 2          # un ticket toutes les 2s
    python ticket_generator.py --brokers localhost:9092
    python ticket_generator.py --count 50             # mode batch : 50 tickets puis stop
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC = "client_tickets"

# --- Données de référence pour la génération aléatoire ---

TYPES_DEMANDE = [
    "Facturation",
    "Support technique",
    "Résiliation",
    "Question générale",
    "Réclamation",
    "Demande d'information",
]

# Priorité pondérée : la plupart des tickets sont "normaux", peu sont critiques
PRIORITES = ["Basse", "Moyenne", "Haute", "Critique"]
PRIORITE_WEIGHTS = [0.30, 0.40, 0.20, 0.10]

DEMANDES = [
    "Je n'arrive pas à me connecter à mon compte.",
    "Ma facture du mois dernier me semble erronée.",
    "Je souhaite résilier mon abonnement.",
    "Le service est indisponible depuis ce matin.",
    "Comment puis-je changer mon moyen de paiement ?",
    "J'ai été facturé deux fois pour le même mois.",
    "Le produit reçu est endommagé.",
    "Je n'ai pas reçu ma confirmation de commande.",
    "Pouvez-vous m'expliquer les nouvelles conditions tarifaires ?",
    "Mon compte a été suspendu sans explication.",
    "J'aimerais mettre à niveau mon abonnement.",
    "Le support technique ne répond pas à mes emails.",
    "Une fonctionnalité ne fonctionne plus depuis la dernière mise à jour.",
    "Je souhaite obtenir un remboursement.",
    "Comment exporter mes données personnelles ?",
]

# Pool fixe de clients pour simuler des clients récurrents
CLIENT_IDS = [f"CUST-{i:04d}" for i in range(1, 201)]


def generate_ticket() -> dict:
    """Génère un ticket client aléatoire."""
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": random.choice(CLIENT_IDS),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "demande": random.choice(DEMANDES),
        "type_demande": random.choice(TYPES_DEMANDE),
        "priorite": random.choices(PRIORITES, weights=PRIORITE_WEIGHTS, k=1)[0],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Producteur de tickets clients vers Redpanda"
    )
    parser.add_argument(
        "--brokers",
        default="localhost:9092",
        help="Adresse(s) du/des broker(s) Redpanda, séparées par des virgules (défaut: localhost:19092)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Délai en secondes entre deux tickets en mode continu (défaut: 1.0)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Nombre de tickets à produire puis s'arrêter (0 = continu, infini)",
    )
    args = parser.parse_args()

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.brokers.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
    except NoBrokersAvailable:
        print(f"Impossible de joindre le(s) broker(s) : {args.brokers}")
        print("Vérifie que le cluster est démarré (`rpk container start -n 3`)")
        print("et l'adresse réelle avec `rpk cluster info` ou `rpk profile print`.")
        return

    mode = (
        "continu (Ctrl+C pour arrêter)" if args.count == 0 else f"{args.count} tickets"
    )
    print(f"Envoi vers le topic '{TOPIC}' sur {args.brokers} — mode : {mode}\n")

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            ticket = generate_ticket()
            # Clé = client_id : garantit que les tickets d'un même client
            # vont toujours dans la même partition (ordre préservé par client)
            producer.send(TOPIC, key=ticket["client_id"], value=ticket)
            sent += 1
            print(
                f"[{sent}] {ticket['ticket_id'][:8]}... | {ticket['client_id']} | "
                f"{ticket['type_demande']} | {ticket['priorite']}"
            )
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        producer.flush()
        producer.close()
        print(f"\nTotal envoyé : {sent} ticket(s).")


if __name__ == "__main__":
    main()
