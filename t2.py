from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re


WORD_RE = re.compile(r"\w+")


class MRLongestWord(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield len(word), word.lower()

    def combiner(self, length, words):
        yield max(length), words

    def reducer(self, length, words):
        yield str(max(length)), str(words)


if __name__ == "__main__":
    MRLongestWord.run()
