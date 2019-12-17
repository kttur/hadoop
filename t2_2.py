from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"\w+")


class MRAverageWordLength(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, (len(word), 1)
            
    def combiner(self, _, lengths):
        yield None, sum(lengths)

    def reducer(self, _, lengths):
        total = sum(lengths)
        yield (float(total[0])/total[1], total[1])


if __name__ == "__main__":
    MRAverageWordLength.run()