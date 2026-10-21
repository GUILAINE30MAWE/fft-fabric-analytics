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

# # Notebook 2 — Nettoyage et cycle de vie du licencié (SQL)
# 
# Ce que fait ce notebook :
# 1. il garde uniquement les licences **validées** (une licence passée à ANNULEE par l'UPSERT disparaît des chiffres) ;
# 2. il nettoie et type les colonnes (espaces, majuscules, dates, montants), calcule l'âge et la catégorie d'âge ;
# 3. avec une **fonction de fenêtre** `LAG`, il regarde si le licencié avait une licence la saison précédente
#    et en déduit son statut : Nouveau, Renouvelé, Réactivé (ou Indéterminé pour la première saison connue) ;
# 4. il écrit des colonnes 0/1 (`flag_renouvele`, `flag_nouveau`, `flag_padel`) : Power BI n'aura qu'à les additionner.
# 
# Rappel métier : la saison « 2027 » va du 1er septembre 2026 au 31 août 2027.

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE stg_licences_valides AS
WITH base AS (
    -- 1. Licences validées, colonnes nettoyées et typées
    SELECT
        TRIM(id_souscription)                                   AS id_souscription,
        TRIM(id_licencie)                                       AS id_licencie,
        CAST(saison AS INT)                                     AS saison,
        TRIM(code_club)                                         AS code_club,
        UPPER(TRIM(genre))                                      AS genre,
        CAST(annee_naissance AS INT)                            AS annee_naissance,
        CAST(saison AS INT) - CAST(annee_naissance AS INT)      AS age_saison,
        INITCAP(TRIM(discipline))                               AS discipline,
        TRIM(type_licence)                                      AS type_licence,
        CAST(montant_licence_eur AS DECIMAL(10,2))              AS montant_licence_eur,
        CAST(date_souscription AS DATE)                         AS date_souscription,
        _batch_date
    FROM raw_licences
    WHERE UPPER(TRIM(statut_licence)) = 'VALIDE'
),
avec_age AS (
    -- 2. Catégorie d'âge (âge au début de la saison)
    SELECT b.*,
        CASE
            WHEN age_saison <= 10 THEN '1. Enfants (10 ans et moins)'
            WHEN age_saison <= 18 THEN '2. Jeunes (11-18 ans)'
            WHEN age_saison <= 34 THEN '3. Adultes (19-34 ans)'
            ELSE                       '4. Adultes (35 ans et plus)'
        END AS categorie_age
    FROM base b
),
avec_historique AS (
    -- 3. Fonction de fenêtre : la saison précédente du même licencié, et la première saison de tout l'historique
    SELECT a.*,
        LAG(saison) OVER (PARTITION BY id_licencie ORDER BY saison) AS saison_precedente,
        MIN(saison) OVER ()                                         AS premiere_saison_historique
    FROM avec_age a
)
-- 4. Statut du cycle de vie et colonnes 0/1 pour Power BI
SELECT
    id_souscription, id_licencie, saison, code_club, genre, annee_naissance, age_saison, categorie_age,
    discipline, type_licence, montant_licence_eur, date_souscription, _batch_date,
    CASE
        WHEN saison = premiere_saison_historique THEN 'Indéterminé (1re saison connue)'
        WHEN saison_precedente IS NULL            THEN 'Nouveau'
        WHEN saison_precedente = saison - 1       THEN 'Renouvelé'
        ELSE                                           'Réactivé'
    END AS statut_fidelite,
    CASE WHEN saison_precedente = saison - 1 THEN 1 ELSE 0 END                                        AS flag_renouvele,
    CASE WHEN saison > premiere_saison_historique AND saison_precedente IS NULL THEN 1 ELSE 0 END      AS flag_nouveau,
    CASE WHEN discipline = 'Padel' THEN 1 ELSE 0 END                                                  AS flag_padel
FROM avec_historique
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Contrôle visuel : volumes par saison et par statut
spark.sql("""
SELECT saison, statut_fidelite, COUNT(*) AS licences
FROM stg_licences_valides
GROUP BY saison, statut_fidelite
ORDER BY saison, statut_fidelite
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
