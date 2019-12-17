from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"\w+")


class MRAverageWordLength(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, (len(word), 1)

    def reducer(self, _, lengths):
        yield "avg len: %f" % float(sum(lengths[0]))/sum(lengths[1]), "total words: %i" % sum(lengths[1])
		
		
if __name__ == "__main__":
    MRAverageWordLength.run()