from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re


class MROneDotAbbr(MRJob):

    OUTPUT_PROTOCOL = TextProtocol
    PATTERN_RE = re.compile(r'(?: |^)\w+\.[,;:?!]?(?: |$)\w?')
    ABBR_RE = re.compile(r"\w+\.")
    THRESHOLD = 0.55

    def mapper(self, _, line):
        for match in self.PATTERN_RE.findall(line):
            if isinstance(match, str):
                for abbr in self.ABBR_RE.findall(match):
                    if isinstance(abbr, str):
                        yield abbr.lower(), match[-1].islower()

    def reducer(self, word, lower_counters):
        total, lower = 0, 0
        for c in lower_counters:
            total += 1
            if c:
                lower += 1
        if total > 10 and float(lower)/total >= self.THRESHOLD:
            yield word, str((total, lower))


if __name__ == "__main__":
    MROneDotAbbr.run()
