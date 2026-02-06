SELECT
    pr.specialty_code,
    YEAR(c.consultation_date) AS consultation_year,
    COUNT(DISTINCT CASE WHEN p.gender = 'F' THEN c.patient_uid END) AS nb_patientes_uniques
FROM consultations c
JOIN practitioners pr
    ON pr.practitioner_uid = c.practitioner_uid
LEFT JOIN patients p
    ON p.patient_uid = c.patient_uid
WHERE c.status IS NULL OR c.status <> 'cancelled'
GROUP BY
    pr.specialty_code,
    YEAR(c.consultation_date)
ORDER BY
    pr.specialty_code,
    consultation_year;

    