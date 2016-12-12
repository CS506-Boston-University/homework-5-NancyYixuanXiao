from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    '''step 1:
    get each word occurrence with review_id
    '''
    def extract_word(self, _, review):
        for word in WORD_RE.findall(review['text']):
            yield (word.lower(), review['review_id'])

    def reduce_word_id(self, word, review_id):
        yield word, review_id

    '''step 2:
    get the unique word over all reviews and reivew_ids
    '''
    def get_uniq_word(self, word, review_id):
        if len(review_id) == 1:
            yield word, review_id[0]

    '''step 3:
    count the number of unique words in each review
    '''
    def map_uniq_review_length(self, word, review_id):
         yield (review_id, 1)

    def combine_uniq_review_length(self, review_id, counts):
        yield (review_id, sum(counts))

    def reduce_max_uniq_length(self, review_id, sum_uniq_pair):
        yield None, (sum(sum_uniq_pair),review_id)

    '''step 4:
    find the review with most unique words
    '''
    def reducer_find_max_word(self, _, uniq_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(uniq_count_pairs)

    def steps(self):
        return [
            MRStep(mapper=self.extract_word,
                   reducer=self.reduce_word_id),
            MRStep(mapper=self.get_uniq_word),
            MRStep(mapper=self.map_uniq_review_length,
                   combiner=self.combine_uniq_review_length,
                   reducer=self.reduce_max_uniq_length),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()