# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee33dd8e-ba56-4069-937a-af3353a55b83",
# META       "default_lakehouse_name": "lh_fft_licences",
# META       "default_lakehouse_workspace_id": "19fbdd05-3fad-4218-a307-ce54ce40e8ef",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee33dd8e-ba56-4069-937a-af3353a55b83"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook 1 — Ingestion incrémentale (Python / PySpark)
# 
# Ce que fait ce notebook, à chaque exécution :
# 1. il regarde dans `Files/input/` s'il y a des dossiers datés (`20261001`, `20261020`...) ;
# 2. pour chaque dossier, il lit les 4 fichiers CSV et met à jour les 4 tables brutes `raw_*`
#    avec un **UPSERT** (MERGE) : une ligne déjà connue est mise à jour, une ligne nouvelle est ajoutée ;
# 3. quand tout s'est bien passé, il déplace le dossier dans `Files/archive/`.
# 
# Si une erreur survient, rien n'est déplacé : le dossier reste dans `input/` et le pipeline s'arrête.

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

INPUT_ROOT = "Files/Input"
ARCHIVE_ROOT = "Files/Archive"

# Les 4 tables brutes et la clé qui identifie une ligne (sert à l'UPSERT)
TABLES = [
    {"table": "raw_ligues",       "cles": ["code_ligue"]},
    {"table": "raw_clubs",        "cles": ["code_club"]},
    {"table": "raw_security_rls", "cles": ["email_user", "code_ligue_autorise"]},
    {"table": "raw_licences",     "cles": ["id_souscription"]},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Quels dossiers datés attendent dans Files/input/ ? (ordre chronologique)
entrees = notebookutils.fs.ls(INPUT_ROOT)

dossiers = []
for item in entrees:
    if item.isDir and item.name.isdigit() and len(item.name) == 8:
        dossiers.append(item.name)

dossiers = sorted(dossiers)

print("Dossiers à traiter :", dossiers if dossiers else "aucun")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Pour chaque dossier : lecture des CSV, UPSERT dans les tables raw_*, puis archivage

def traiter_dossier(dossier: str) -> None:
    """Traite un dossier (YYYYMMDD) : charge les CSV, fait l'UPSERT, puis archive le dossier."""
    chemin_input = f"{INPUT_ROOT}/{dossier}"
    chemin_archive = f"{ARCHIVE_ROOT}/{dossier}"
    print(f"\n=== Dossier {dossier} ===")

    for cfg in TABLES:
        table = cfg["table"]
        cles = cfg["cles"]

        # a) lecture du CSV et ajout des colonnes techniques
        chemin_fichier = f"{chemin_input}/{table}.csv"
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(chemin_fichier)
            .withColumn("_batch_date", F.lit(dossier))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        # b) suppression des doublons sur la clé
        df = df.dropDuplicates(cles)

        # c) création ou UPSERT dans la table Delta
        if not spark.catalog.tableExists(table):
            # première exécution : on crée la table
            df.write.format("delta").mode("overwrite").saveAsTable(table)
            print(f"  {table} : table créée ({df.count()} lignes)")
        else:
            # exécution suivante : MERGE (UPSERT)
            delta_table = DeltaTable.forName(spark, table)
            condition = " AND ".join([f"t.{c} = s.{c}" for c in cles])

            (
                delta_table.alias("t")
                .merge(df.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"  {table} : MERGE effectué ({df.count()} lignes)")

    # d) archivage du dossier si tout s'est bien passé
    notebookutils.fs.mv(chemin_input, chemin_archive, create_path=True, overwrite=True)
    print(f"  Dossier archivé vers {chemin_archive}")


# Boucle sur tous les dossiers à traiter
for dossier in dossiers:
    traiter_dossier(dossier)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
