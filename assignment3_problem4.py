
import csv
from whoosh.index import create_in
from whoosh.fields import *
from whoosh import scoring
from whoosh.qparser import QueryParser

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def read_file(file_path, delimiter='\t'):
    with open(file_path, 'r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
        doc_list = []
        for row in reader:
            doc_list.append((row[0],row[1], row[2].replace('\n',' ')))

    return doc_list

doc_list = read_file("collection.tsv")

print(len(doc_list),'\n', doc_list[0])


schema = Schema(id=ID(stored=True), content=TEXT)
ix = create_in("c_index", schema)
writer = ix.writer()


for doc in doc_list[:1000]:
    writer.add_document(id=doc[0],content=doc[2])
writer.commit()


query_str =  "eagle"
result_list = []




#(a) OWN RANKINGS:

#OWN tf-idf ranking 
def own_tf_idf_score(searcher, fieldname, text, matcher):
    termCountAllDocs = searcher.frequency(fieldname, text)
    freqs = matcher.value_as("frequency")
    tf_idf = freqs/termCountAllDocs
#    print(text)
#    print(termCountAllDocs)
#    print(poses)
#    print()
    return tf_idf

#term frequency * term position
def pos_freq_score(searcher, fieldname, text, matcher):
    poses = matcher.value_as("positions")
    freqs = matcher.value_as("frequency")
    value = freqs * (1.0 / (poses[0] + 1))
#    print(poses)
#    print(freqs)
#    print(value)
#    print()
    return value

#change score function here
w = scoring.FunctionWeighting(pos_freq_score)

with ix.searcher(weighting =w) as searcher:
    query = QueryParser("content", ix.schema).parse(query_str)
    results = searcher.search(query, limit=None)
    print("Results found:", len(results))
    for result in results:
        print(result['id'], result.score)
        result_list.append(result['id'])



#(b) begin...
def read_qrels(file_path, delimiter=' '):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
        qrels = {}
        for row in reader:
            qrels[row[0].replace('\n',' ')] = int(row[2])

    return qrels

qrels_hash = read_qrels("q5.web.qrels.txt")

def precision(doc_list, qrels, k=10):
    f = lambda x: qrels[x] if x in qrels else 0
    vals = list(map(lambda q: 1 if q>0 else 0, map(f, doc_list[:k])))
    print(vals)
    return sum(vals)/k

precision(result_list, qrels_hash)