from mrjob.job import MRJob
import re

class MrJobImplementation(MRJob):

    def mapper(self, _, line):
        num_words = len(line.split())
        words = line.split()
        for i in range(num_words - 1):
            bigram = f"{words[i].lower()},{words[i + 1].lower()}"
            yield bigram,1
     
    def reducer(self, bigram, counts):
        yield bigram, sum(counts)


MrJobImplementation.run()
