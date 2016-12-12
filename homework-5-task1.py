from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class ReviewWordCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    
    def extract_words(self, _, review):
        """Extract words using a regular expression.  Normalize the text to
        ignore capitalization."""
        for word in WORD_RE.findall(review['text']):
            yield (word.lower(), 1)

    def count_words(self, word, counts):
        """Summarize all the counts by taking the sum."""
        yield (word, sum(counts))

    def steps(self):
        return [MRStep(mapper=self.extract_words,
                       reducer=self.count_words)]

if __name__ == '__main__':
    ReviewWordCount.run()