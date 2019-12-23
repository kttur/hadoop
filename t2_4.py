# 4. Все слова, которые более чем в половине случаев начинаются с большой буквы и встречаются больше 10 раз.

from mrjob.job import MRJob
import re


WORD_RE = re.compile(r"\w+")


class MRUppercaseWords10(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), word[0].isupper()

    def reducer(self, word, upper_counters):
        total, upper = 0, 0
        for c in upper_counters:
            total += 1
            if c:
                upper += 1
        if total >= 10 and float(upper)/total >= 0.5:
            yield word, str((total, upper))


if __name__ == "__main__":
    MRUppercaseWords10.run()
