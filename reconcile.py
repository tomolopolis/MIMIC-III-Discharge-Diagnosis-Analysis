from typing import List, Dict

import pandas as pd


def map_medcat_annos_to_icd9(annos: List[Dict], notes: pd.DataFrame,
                             codes: pd.DataFrame, cuis_to_keep: List[str]) -> pd.DataFrame:
    """
    Takes MedCAT produced JSON and maps produced ICD-9 codes to note.hadm, ensures predicted CUIs are in
    the cuis_to_keep mapped to top_400 ICD9 codes. Many-to-many ICD-10 to ICD-9 results in duplicate rows
    of the same text span.
    Then maps UMLS CUIs to ICD-9 codes through ICD-10.
    :param annos: MedCAT produced JSON
    :param notes: MIMIC-III DD notes, with associated HADM ID.
    :param cuis_to_keep: UMLS CUIs to keep.
    :param codes: MIMIC-III assigned ICD-9 codes.
    :return:
    """
    all_rows = []
    for note, ents in zip(notes.itertuples(), annos):
        for e in ents['entities']:
            if e['cui'] in cuis_to_keep:
                e['hadm'] = note.name
                all_rows.append(e)
    df = pd.DataFrame(all_rows)
    sorted_top_400 = codes.groupby('icd9_code').count().loc[:, 'hadm_id'].sort_values(
        ascending=False)[0:400].index.tolist()
    icd9_to_10_mapping = pd.read_csv('refdata/icd9toicd10cmgem.csv')
    icd9_to_10_mapping = icd9_to_10_mapping[icd9_to_10_mapping.icd9cm.isin(sorted_top_400)]
    filtered_icd_10_codes = icd9_to_10_mapping.icd10cm.tolist()

    parsed_dd_mapped_to_icd9 = []
    no_mapping_found = []
    for r in df.itertuples():
        for code in r.icd10.split(','):
            clean_code = code.replace('.', '')
            if clean_code in filtered_icd_10_codes:
                mapped_icd9_codes = icd9_to_10_mapping[icd9_to_10_mapping.icd10cm == clean_code].icd9cm.tolist()
                mapped_icd9_code_instances = [{
                    'acc': r.acc,
                    'cui': r.cui,
                    'hadm': r.hadm,
                    'pretty_name': r.pretty_name,
                    'source_value': r.source_value,
                    'start': r.start,
                    'end': r.end,
                    'icd9': icd9_code,
                    'icd10': code
                } for icd9_code in mapped_icd9_codes]
                parsed_dd_mapped_to_icd9 += mapped_icd9_code_instances
            else:
                no_mapping_found.append(code)
    parsed_dd_mapped_to_icd9 = pd.DataFrame(parsed_dd_mapped_to_icd9)
    return parsed_dd_mapped_to_icd9


def remove_duplicate_mapped_assigned_codes(notes: pd.DataFrame, codes: pd.DataFrame,
                                           parsed_dd_mapped_to_icd9: pd.DataFrame) -> pd.DataFrame:
    """
    As UMLS codes can map many-to-many to ICD-10 and ICD-9 codes, a single UMLS prediction by MedCAT can map
    to many associated ICD codes. If the MIMIC-III assigned codes matches one of these UMLS assigned codes we
    assume the MIMIC-III assigned code is correct and remove all others that refer to the same span of text.
    :param codes: MIMIC-III assigned ICD-9 codes.
    :param parsed_dd_mapped_to_icd9: Predicted codes from MedCAT mapped to ICD-9.
    :return:
    """
    sorted_top_400 = codes.groupby('icd9_code').count().loc[:, 'hadm_id'].sort_values(
        ascending=False)[0:400].index.tolist()
    codes = codes[codes.icd9_code.isin(sorted_top_400)]
    codes = codes[codes.hadm_id.isin(notes.name)]
    codes = codes.reset_index(drop=True)
    codes.columns = ['hadm', 'icd9', 'short_title', 'long_title']
    joined_df = []
    for h_key, codes_by_hadm in codes.groupby('hadm'):
        pred_codes = parsed_dd_mapped_to_icd9[parsed_dd_mapped_to_icd9.hadm == str(h_key)]
        rows_to_keep = []
        mult_starts = []
        matched_icd9_codes = []
        for code_row in codes_by_hadm.itertuples():
            if code_row.icd9 in pred_codes.icd9.tolist():
                # true positive rows. De-dupe is needed.
                pred_code_match = pred_codes[pred_codes.icd9 == code_row.icd9].iloc[0]
                pred_code_match = pred_code_match.to_dict()
                pred_code_match['match'] = 'match'
                matched_icd9_codes.append(pred_code_match['icd9'])
                rows_to_keep.append(pred_code_match)
                mult_starts.append(pred_code_match['start'])
        no_match_pred_codes = pred_codes[~pred_codes.start.isin(mult_starts)]
        no_match_pred_codes = no_match_pred_codes.to_dict(orient='records')
        for p in no_match_pred_codes:
            p['match'] = 'pred_no_assign'
        rows_to_keep.extend(no_match_pred_codes)

        no_match_codes = codes_by_hadm[~codes_by_hadm.icd9.isin(matched_icd9_codes)].loc[:, ['hadm', 'icd9']]
        no_match_codes = no_match_codes.to_dict(orient='records')
        for p in no_match_codes:
            p['match'] = 'assigned_no_pred'
        rows_to_keep.extend(no_match_codes)
        joined_df.extend(rows_to_keep)
    pd.DataFrame(joined_df)
    return joined_df
