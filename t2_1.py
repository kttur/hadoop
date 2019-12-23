# 1. Напишите программу, которая находит самое длинное слово.

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re


WORD_RE = re.compile(r"\w+")


class MRLongestWord(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, word

    def combiner(self, _, words):
        yield None, max(words, key=lambda w: len(w))

    def reducer(self, _, words):
        max_word = max(words, key=lambda w: len(w))
        yield max_word, str(len(max_word))
		
		
if __name__ == "__main__":
    MRLongestWord.run()