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

# # Notebook 3 — Modèle en étoile, table de sécurité et tests (SQL)
# 
# Ce que fait ce notebook :
# 1. il construit les tables que Power BI va lire : `dim_clubs_ligues`, `dim_calendrier`, `fct_licences`
#    et la table de sécurité `sec_rls_utilisateurs_ligues` (qui a le droit de voir quelle ligue) ;
# 2. il calcule une table de référence `kpi_saison` pour vérifier les chiffres de Power BI ;
# 3. il exécute **8 tests** ; si un seul échoue, le notebook s'arrête en erreur et le pipeline ne rafraîchit pas Power BI.
# 
# Aucune donnée nominative dans ces tables (pas de nom, pas d'email de licencié).

# CELL ********************

# 1. Dimension clubs et ligues (hiérarchie ligue > club, équipements)
spark.sql("""
CREATE OR REPLACE TABLE dim_clubs_ligues AS
SELECT
    TRIM(c.code_club)                                      AS code_club,
    TRIM(c.nom_club)                                       AS nom_club,
    UPPER(TRIM(c.code_ligue))                              AS code_ligue,
    l.nom_ligue,
    l.zone_geo,
    CAST(c.nb_courts_tennis AS INT)                        AS nb_courts_tennis,
    CAST(c.nb_pistes_padel AS INT)                         AS nb_pistes_padel,
    CASE WHEN CAST(c.nb_pistes_padel AS INT) > 0 THEN 'Club équipé padel' ELSE 'Tennis uniquement' END AS equipement_padel
FROM raw_clubs c
LEFT JOIN raw_ligues l ON UPPER(TRIM(c.code_ligue)) = UPPER(TRIM(l.code_ligue))
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Dimension calendrier : un jour par ligne, du 1er septembre 2024 au 31 août 2027, avec la saison FFT
spark.sql("""
CREATE OR REPLACE TABLE dim_calendrier AS
SELECT
    d                                                                  AS date_key,
    YEAR(d)                                                            AS annee,
    MONTH(d)                                                           AS mois,
    DATE_FORMAT(d, 'yyyy-MM')                                          AS annee_mois,
    CASE WHEN MONTH(d) >= 9 THEN YEAR(d) + 1 ELSE YEAR(d) END          AS saison,
    CASE WHEN MONTH(d) >= 9 THEN MONTH(d) - 8 ELSE MONTH(d) + 4 END    AS mois_de_saison
FROM (SELECT EXPLODE(SEQUENCE(DATE '2024-09-01', DATE '2027-08-31', INTERVAL 1 DAY)) AS d)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Table de sécurité : une ligne par utilisateur et par ligue autorisée ('ALL' = toutes les ligues)
spark.sql("""
CREATE OR REPLACE TABLE sec_rls_utilisateurs_ligues AS
SELECT DISTINCT
    LOWER(TRIM(r.email_user))   AS email_user,
    l.code_ligue,
    r.role_description
FROM raw_security_rls r
JOIN raw_ligues l
  ON UPPER(TRIM(r.code_ligue_autorise)) = l.code_ligue
  OR UPPER(TRIM(r.code_ligue_autorise)) = 'ALL'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Table de faits : une ligne par licence validée
spark.sql("""
CREATE OR REPLACE TABLE fct_licences AS
SELECT
    s.id_souscription,
    s.id_licencie,
    s.saison,
    s.code_club,
    c.code_ligue,
    s.date_souscription        AS date_key,
    s.genre,
    s.age_saison,
    s.categorie_age,
    s.discipline,
    s.type_licence,
    s.statut_fidelite,
    s.flag_renouvele,
    s.flag_nouveau,
    s.flag_padel,
    s.montant_licence_eur
FROM stg_licences_valides s
LEFT JOIN dim_clubs_ligues c ON s.code_club = c.code_club
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Table de référence par saison : les chiffres que Power BI doit retrouver exactement
spark.sql("""
CREATE OR REPLACE TABLE kpi_saison AS
WITH par_saison AS (
    SELECT saison,
           COUNT(*)              AS licences,
           SUM(flag_nouveau)     AS nouveaux,
           SUM(flag_renouvele)   AS renouveles,
           SUM(flag_padel)       AS licences_padel,
           SUM(montant_licence_eur) AS montant_eur
    FROM fct_licences
    GROUP BY saison
)
SELECT p.*,
       LAG(licences) OVER (ORDER BY saison)                                  AS licences_n_1,
       ROUND(licences_padel / licences, 4)                                   AS part_padel,
       ROUND((licences - LAG(licences) OVER (ORDER BY saison)) / LAG(licences) OVER (ORDER BY saison), 4) AS evolution_n_1,
       ROUND(renouveles / LAG(licences) OVER (ORDER BY saison), 4)           AS taux_retention,
       ROUND(1 - renouveles / LAG(licences) OVER (ORDER BY saison), 4)       AS taux_churn
FROM par_saison p
ORDER BY saison
""")
spark.table("kpi_saison").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Tests : chaque requête doit renvoyer 0. Un seul écart arrête le notebook (et donc le pipeline).
TESTS = {
    "Identifiants de licence uniques dans fct_licences":
        "SELECT COUNT(*) - COUNT(DISTINCT id_souscription) FROM fct_licences",
    "Chaque licence est rattachée à un club connu":
        "SELECT COUNT(*) FROM fct_licences WHERE code_ligue IS NULL",
    "Autant de licences dans fct_licences que de licences validées en staging":
        "SELECT (SELECT COUNT(*) FROM stg_licences_valides) - (SELECT COUNT(*) FROM fct_licences)",
    "Aucun montant négatif ou nul":
        "SELECT COUNT(*) FROM fct_licences WHERE montant_licence_eur <= 0",
    "Chaque licence est datée dans sa saison (1er septembre -> 31 août)":
        "SELECT COUNT(*) FROM fct_licences f JOIN dim_calendrier d ON d.date_key = f.date_key WHERE d.saison <> f.saison",
    "Statut de fidélité dans la liste autorisée":
        "SELECT COUNT(*) FROM fct_licences WHERE statut_fidelite NOT IN ('Indéterminé (1re saison connue)', 'Nouveau', 'Renouvelé', 'Réactivé')",
    "Un licencié n'a qu'une licence par saison":
        "SELECT COUNT(*) FROM (SELECT id_licencie, saison FROM fct_licences GROUP BY id_licencie, saison HAVING COUNT(*) > 1)",
    "La direction nationale voit toutes les ligues":
        "SELECT (SELECT COUNT(*) FROM raw_ligues) - (SELECT COUNT(*) FROM sec_rls_utilisateurs_ligues WHERE email_user = (SELECT MIN(LOWER(TRIM(email_user))) FROM raw_security_rls WHERE UPPER(TRIM(code_ligue_autorise)) = 'ALL'))",
}

resultats = []
for nom, sql in TESTS.items():
    ecart = spark.sql(sql).collect()[0][0] or 0
    statut = "OK" if ecart == 0 else "KO"
    resultats.append((nom, statut, int(ecart)))
    print(f"[{statut}] {nom} (écart : {ecart})")

spark.createDataFrame(resultats, ["test", "resultat", "ecart"]).write.format("delta").mode("overwrite").saveAsTable("qualite_tests")

nb_ko = sum(1 for _, statut, _ in resultats if statut == "KO")
if nb_ko > 0:
    raise Exception(f"{nb_ko} test(s) en échec : le modèle n'est pas publié.")
print("Tous les tests sont OK : le modèle peut être rafraîchi dans Power BI.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
