#/usr/bin/python
#coding:utf-8
#author dimstunt
import re
import string
import pyspark
from pprint import pprint
import numpy as np

SEP = '"========================================================================'


def load_documents():
    with open('facts.txt') as input_file:
        documents = input_file.read().split(SEP)
    documents = map(lambda doc: doc.strip('"' + string.whitespace), documents)
    documents = filter(lambda doc: doc and not doc.isspace(), documents)
    return documents




COUNTRY_RE = re.compile('(?P<name>[A-Za-z ]+)\n\nIntroduction')
RAIL_RE = re.compile('Railways:[ ]+total:[ ]+(?P<len>[0-9\.\,]+)?([ ]+km[ ]+)?(([a-zA-Z]+ )*)?(?P<type>[a-zA-Z]+)[ ]+gauge')
SEARCH_IM_RE = re.compile('Imports - commodities:  (.*?)\n\n', re.S)
SPLIT_RE = re.compile('(?:and|,|;|\n)')

def extract_railways(document):
    country = COUNTRY_RE.search(document)
    if country is None:
        return []

    intern = RAIL_RE.search(document)
    if intern is None:
        return []

    if country is not None:
        return [(
            intern.group('type').lower(),
            float(intern.group('len').replace(",",""))
        )]

def extract_imports(document):
    country = COUNTRY_RE.search(document)
    if country is None:
        return []

    imports = SEARCH_IM_RE.search(document)
    if imports is None:
        return []

    split = SPLIT_RE.split(imports.group(1))
    return [(
        country.group('name').lower(),
        len(split) if split is not None else 0
    )]


def run_spark_2(sc, docs_rdd, extractor):
    result = docs_rdd.flatMap(extractor)
    pprint(result.reduceByKey(lambda a, b: a + b).collect())


def run_spark_5(sc, docs_rdd, extractor):
    result = docs_rdd.flatMap(extractor)
    pprint(result.collect())


if __name__ == '__main__':
    documents = load_documents()
    sc = pyspark.SparkContext('local', 'Regex App')
    documents_rdd = sc.parallelize(documents)
    print('\nTask 2:')
    run_spark_2(sc, documents_rdd, extract_railways)
    print('\nTask 5:')
    run_spark_5(sc, documents_rdd, extract_imports)
