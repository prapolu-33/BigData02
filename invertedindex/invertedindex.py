from mrjob.job import MRJob
import re

class Inverted_Index(MRJob):

    def mapper(self, _, line):
    
        parts = line.split(":", 1)
        if len(parts) != 2:
            return

        document_id, document = parts
        document_id = document_id.strip()

        words = line.split()

        for word in words:
            yield word, document_id

    def reducer(self, word, doc_ids):
    
        doc_id_set = set(doc_ids)

        doc_id_str = ", ".join(sorted(doc_id_set))

        yield word, doc_id_str

if __name__ == "__main__":
    Inverted_Index.run()
