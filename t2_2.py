from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"\w+")


class MRAverageWordLength(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, len(word)

    def reducer(self, _, lengths):
        yield "avg len: %f" % float(sum(lengths))/len(lengths), "total words: %i" % len(lengths)
		
		
if __name__ == "__main__":
    MRAverageWordLength.run()