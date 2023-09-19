from mrjob.job import MRJob
import re

class mrimplement(MRJob):
    
    def mapper(self, _ , line):
        for word in line.split():
            yield (word.lower(), 1)
            
    def reducer(self, word, counts):
        yield (word, sum(counts))
        
        
mrimplement.run()

