from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"\w+")


class MRAverageWordLength(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, len(word)

    def reducer(self, _, lengths):
        yield f"avg len: {sum(lengths)/len(lengths)}", f"total words: {len(lengths)}"
		
		
if __name__ == "__main__":
    MRAverageWordLength.run()