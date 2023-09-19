from mrjob.job import MRJob
import re

stop_words = ['the','and','of','a','to','in','is','it']

class mrimplement(MRJob):
    
    def mapper(self, _ , line):
        for word in line.split():
            if word.lower() not in stop_words:
                yield (word.lower(), 1)
            
    def reducer(self, word, counts):
        yield (word, sum(counts))
        
        
mrimplement.run()


