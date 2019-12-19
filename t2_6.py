from mrjob.job import MRJob
from t2_5 import MROneDotAbbr
import re


class MROneLetterAbbr(MROneDotAbbr):

    PATTERN_RE = re.compile(r'(?: |^)(?:\w\.)+[,;:"?!]?(?: |$)\w?')
    ABBR_RE = re.compile(r'(?:\w\.)+')


if __name__ == "__main__":
    MROneLetterAbbr.run()
