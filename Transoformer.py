import json


class Transoformer():
    """
 Transoformer perform the needed transformation to process the dataframe and get the desired result
 ---------
 Constructor :
 drugs : Drugs Dataframe
 pubmed : Medical publication  dataframe
 clinical_trials : Clinical Trials dataframe
 """

    def __init__(self, drugs, pubmed, clinical_trials):
        self.drugs = drugs
        self.pubmed = pubmed
        self.clinical_trials = clinical_trials

    def __call__(self, *args, **kwargs):
        return self.transform_data()


"""
Methode called to resume the transformation jobs

"""


def transform_data(self):
    result = self.drugs.copy()
    result['journal'] = [{}] * len(result)
    result = self.find_in(result=result, df=self.pubmed, container="title", column="Pub_med_mentions")
    result = self.find_in(result=result, df=self.clinical_trials, container="scientific_title",
                          column="Clinicals_mentions")
    return result


"""
Methode called in transform_data
it check in dataframe "df", in the column " container", if the drug exist from the list of drugs   

Result will is a dataframe:
atccode : drug id 
drug : drug name 
journal : json that describes the  journal in which the drug was mentioned 
Pub_med_mentions : json that describes the  pubmed in which the drug was mentioned 
Pub_med_mentions : json that describes the  pubmed in which the drug was mentioned 
Clinicals_mentions: json that describes the  Clinical trials in which the drug was mentioned

"""


def find_in(self, result, df, container, column):
    for drug in result["drug"]:
        where = df[df[container].str.contains(drug, na=False, case=False)]  #
        result.loc[result.drug == drug, column] = where[['id', container, 'date', 'journal']].to_json(orient="records")
        old_journals = json.loads(result.loc[result.drug == drug, 'journal'].to_json(orient="records"))
        new_journals = json.loads(where[['id', container, 'date', 'journal']].to_json(orient="records"))
        result.loc[result.drug == drug, 'journal'] = json.dumps(list(old_journals[0]) + new_journals)
    result[column] = result[column].apply(json.loads)
    result['journal'] = result['journal'].apply(json.loads)
    return result
