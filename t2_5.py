from mrjob.job import MRJob
import re


class MROneDotAbbr(MRJob):

    PATTERN_RE = re.compile(r'( |^)\w+\.[,;:"?!]?( |$)"?\w?')
    ABBR_RE = re.compile(r'\w+\.')
    REMOVED_SYMBOLS = re.compile('[,;:"?!]+')

    def mapper(self, _, line):
        for match in self.PATTERN_RE.findall(line):
            if match:
                clean_match = re.sub(r'[,;:"?!]+', "", match)
                yield self.ABBR_RE.search(match).lower(), clean_match[-1].isupper()

    def reducer(self, word, upper_counters):
        total, upper = 0, 0
        for c in upper_counters:
            total += 1
            if c:
                upper += 1
        if float(upper)/total <= 0.5:
            yield word, str((total, upper))


if __name__ == "__main__":
    MROneDotAbbr.run()
