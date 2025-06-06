
# Note: activate virtual environment: venv_ruslaw\Scripts\activate
# -*- coding: utf-8 -*- 
# By Denis A.Saveliev
# This script loads RusLawOD data to a Pandas DataFrame
# It works in Python 3

import pandas as pd
import xml.etree.ElementTree as ET
import glob
from tqdm import tqdm
import pyarrow

work_dir = ''  # path
datasetdir = work_dir + 'corpus_xml_full/corpus_xml_lite/'  # your path here

# different types of work for different types of data 
# fieldsid = ['pravogovruNd',
#             'issuedByIPS',
#             'docdateIPS' ,
#             'docNumberIPS',
#             'doc_typeIPS',
#             'headingIPS',
#             'doc_author_normal_formIPS',
#             'signedIPS',
#             'statusIPS',
#             'actual_datetimeIPS',
#             'actual_datetime_humanIPS',
#             'is_widely_used',
#             ]

# different types of work for different types of data 
fields_keep = [
    'pravogovruNd',          # Unique ID
    'docTitleByOP',          # Title (preferred)
    'headingIPS',            # Fallback title
    'docDateByOP',           # Date (preferred)
    'docdateIPS',            # Fallback date
    'authorByOP',            # Issuing authority (preferred)
    'docTypeByIPS',          # Fallback: includes type + issuing authority
    'docNumberByOP',         # Legal number (from OP)
    'docnumberIPS',          # Legal number (from IPS)
    'docTypeByOP',           # Type from OP
    'classifierByIPS'        # Classification
    'keywordByIPS'           # Keywords
]
fieldshd = ['headingIPS', ]
fieldstxt = ['taggedtextIPS',  'textIPS']
fieldsclass = ['classifierByIPS',  'keywordsByIPS']

def parse_file(inputfilename):
    tree = ET.parse(inputfilename)
    root = tree.getroot()
    bufferdict = {}

    for field in fields_keep:
        # identification block
        if field in ['pravogovruNd', 'docdateIPS', 'docnumberIPS', 'docTypeByIPS', 'headingIPS']:
            try:
                bufferdict[field] = root.find("./meta/identification/" + field).get('val')
            except:
                bufferdict[field] = None

        # official publication block
        elif field in ['docTitleByOP', 'docDateByOP', 'docNumberByOP', 'docTypeByOP', 'authorByOP']:
            try:
                bufferdict[field] = root.find("./meta/identification/" + field).get('val')
            except:
                bufferdict[field] = None

        # classifier
        elif field == 'classifierByIPS':
            try:
                cla = root.findall("./meta/reference/classifierByIPS")
                val = ';'.join(i.get('val') for i in cla if i.get('val'))
                bufferdict[field] = val if val else None
            except:
                bufferdict[field] = None

    return bufferdict

if __name__ == "__main__":
    alldata = []

    xml_files = glob.glob(datasetdir + '*.xml')

    for inputfilename in tqdm(xml_files, desc="Parsing XML files"):
        bufferdict = parse_file(inputfilename)
        alldata.append(bufferdict)


    df = pd.DataFrame(alldata, columns=fields_keep)
    df.to_parquet("ru_full_original_1991_2023.parquet", engine="pyarrow")
    print(f"Total number of rows: {len(df)}")
    print("✅ Saved to ru_full_original_1991_2023.parquet")
