import logging
import os

import pandas as pd
import numpy as np

import psycopg2
import psycopg2.extras


logger = logging.getLogger()

MIMIC_2 = 'mimic2'
MIMIC_3 = 'mimic3'


def mimic2_diagnosis_icd(conn):
    logger.info("Pulling all ICD9 Codes for MIMIC2 Admissions")
    columns = ['subject_id', 'hadm_id', 'sequence', 'icd9_code', 'short_title']
    query = "select subject_id, hadm_id, sequence, replace(code, '.', '') as icd9_code, " \
            "description as short_title from icd9"
    return _pull_from_db(conn, [c.split(' ')[-1] for c in columns], query, MIMIC_2,
                         'admission_codes.csv')


def mimic2_notes(conn):
    logger.info("Pulling all Discharge Summaries for MIMIC2")
    columns = ['subject_id', 'hadm_id', 'charttime', 'category', 'text']
    query = f"select {', '.join(columns)} " \
        " from noteevents where category = 'DISCHARGE_SUMMARY'" \
        " and regexp_replace(noteevents.text, '\s+', '', 'g') <> ''"
    return _pull_from_db(conn, columns, query, MIMIC_2, 'notes.csv')


def mimic3_diagnosis_icd(conn):
    logger.info("Pulling ICD9 Diagnosis codes for MIMIC3 Admissions")
    columns = ['hadm_id', 'subject_id', 'sequence', 'icd9_code', 'short_title', 'long_title']
    query = """
        select d_icd.hadm_id, d_icd.subject_id, d_icd.seq_num, 
            icd.icd9_code, icd.short_title, icd.long_title
        from DIAGNOSES_ICD as d_icd, D_ICD_DIAGNOSES as icd
        where d_icd.icd9_code = icd.icd9_code
    """
    return _pull_from_db(conn, columns, query, MIMIC_3, 'admission_codes.csv')


def mimic3_note_adult_only(conn):
    query = """
    select * from (
        select EXTRACT(YEAR from a.admittime) - EXTRACT(YEAR from p.dob) AS age from admissions as a, patients as p where a.subject_id = p.subject_id
    ) as ages
    where ages.age > 18;
    """
    return _pull_from_db(conn, ['*'], query, MIMIC_3, 'notes.csv')


def mimic3_notes(conn):
    logger.info("Pulling all Discharge Summaries for MIMIC3")
    columns = ['subject_id', 'hadm_id', 'chartdate', 'category', 'description', 'text']
    query = f"select {', '.join(columns)}  from NOTEEVENTS where category='Discharge summary'"
    return _pull_from_db(conn, columns, query, MIMIC_3, 'notes.csv')


def _pull_from_db(conn, columns, query, data_src, filename):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    logger.info(f"Executing Query:\n{query}")
    cursor.execute(query)
    data = pd.DataFrame(np.array(cursor.fetchall()), columns=columns)
    os.makedirs(f'data/{data_src}/raw', exist_ok=True)
    data.to_csv(f'data/{data_src}/raw/{filename}', index=False)
    logger.info(f'Written {filename} to working dir .')
    cursor.close()
    return data
