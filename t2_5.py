from mrjob.job import MRJob
import re


class MROneDotAbbr(MRJob):

    PATTERN_RE = re.compile(r'(?m)( |^)[a-zA-Zа-яА-Я]+\.[,;:"?!]?( |$)"?\w?')
    ABBR_RE = re.compile(r"[a-zA-Zа-яА-Я]+\.")
    REMOVED_SYMBOLS = re.compile("[,;:\"?!]+")
    THRESHOLD = 0.7

    def mapper(self, _, line):
        for match in self.PATTERN_RE.findall(line):
            if match is str:
                for abbr in self.ABBR_RE.findall(match):
                    if abbr is str:
                        yield abbr.lower(), match[-1].islower()

    def reducer(self, word, lower_counters):
        total, lower = 0, 0
        for c in lower_counters:
            total += 1
            if c:
                lower += 1
        if float(lower)/total >= self.THRESHOLD:
            yield word, str((total, lower))


if __name__ == "__main__":
    MROneDotAbbr.run()
