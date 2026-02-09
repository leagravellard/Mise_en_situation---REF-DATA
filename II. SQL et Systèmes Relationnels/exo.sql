/* ============================================================
   Contexte (exercice SQL)
   Objectif : analyser l’évolution de la fréquentation féminine par année,
   en listant pour chaque couple (spécialité du praticien, année) :
   - la spécialité (specialty_code)
   - l’année de consultation (extrait de consultation_date)
   - le nombre de patientes uniques (gender = 'F')
   Contraintes :
   - exclure les consultations annulées (status <> 'cancelled')
   - exhaustivité : garder tous les couples spécialité+année ayant eu des consultations
   - un patient_uid peut exister dans consultations sans être dans patients
   ============================================================ */


/* =======================
   1) SELECT : champs à afficher + métrique
   ======================= */
SELECT
    pr.specialty_code,
    YEAR(c.consultation_date) AS consultation_year,
    COUNT(DISTINCT CASE WHEN p.gender = 'F' THEN c.patient_uid END) AS nb_patientes_uniques

/* =======================
   2) FROM + JOIN : sources et relations
   - consultations = table pivot (une ligne = une consultation)
   - JOIN practitioners pour récupérer la spécialité
   - LEFT JOIN patients car certains patient_uid peuvent être absents de patients
   ======================= */
FROM consultations c
JOIN practitioners pr
    ON pr.practitioner_uid = c.practitioner_uid
LEFT JOIN patients p
    ON p.patient_uid = c.patient_uid

/* =======================
   3) Filtre + Agrégation
   - Filtrer les consultations non annulées
   - Grouper par spécialité et année
   - Trier pour lecture
   ======================= */
WHERE c.status <> 'cancelled'
GROUP BY
    pr.specialty_code,
    YEAR(c.consultation_date)
ORDER BY
    pr.specialty_code,
    consultation_year;
