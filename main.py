import os
import json
from Extractor import Extractor
from Transoformer import Transoformer
import sys

DATA_DIR = os.path.relpath("./input")
DESCRIPTION_DIR = os.path.relpath("./input/input_description")
Output_DIR = os.path.relpath("./output")

# LOGGING
sys.stdout = open('./logs/pipeline_log.txt', 'w')
sys.stderr = open('./logs/error_log.txt', 'w')


def ad_hoc_processing(json_file) -> str:
    with open(json_file, "r") as json_file:
        result = json.load(json_file)
        journals = {}
        for element in result:
            for drug in element['journal']:
                journal = drug['journal']
                if journal not in journals.keys():
                    journals[journal] = []
                if element['drug'] not in journals[journal]:
                    journals[journal].append(element['drug'])
        journals = dict(
            sorted({key: len(journals[key]) for key in journals}.items(), key=lambda item: item[1], reverse=True))
        most_mentioned_journal = next(iter(journals.keys()))
        # the journal with the most mentions of different drugs
        # is the first journal
        return journals, most_mentioned_journal


def main():
    # Data Extracting
    data_retriever_object = Extractor(data_dir=DATA_DIR, description_dir=DESCRIPTION_DIR)
    datasets = data_retriever_object()
    # Data Transformation
    data_transformer_object = Transoformer(drugs=datasets['drugs'], pubmed=datasets['pubmed'],
                                           clinical_trials=datasets['clinical_trials'])
    Result = data_transformer_object()
    # Data Loading
    result = json.dumps(json.loads(Result.to_json(orient="records")), indent=4)
    with open(os.path.join(Output_DIR, "Output_pipeline.json"), 'w') as outfile:
        outfile.write(result)
    # Add-hoc process
    with open(os.path.join(Output_DIR, "Output_ad_hoc.txt"), 'w') as outfile:
        journals, most_mentioned_journal = ad_hoc_processing("./output/Output_pipeline.json")
        outfile.write("the most mentioned jounal is " + most_mentioned_journal + "\n")
        outfile.write(" Here you have json  of the Journal and the number of different drug mentioned in them \n" +
                  str(journals))
    print("The Pipeline is Done successfully")


if __name__ == '__main__':
    main()
