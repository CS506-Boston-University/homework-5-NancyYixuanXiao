from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class ReviewWordCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    
    # '''Step 1:
    # First mapreduce: user as key, group business by users
    # '''
    def map_users(self, _, review):
        user = review['user_id']
        business = review['business_id']
        yield user, business

    def combine_user_business(self, user, business):
        yield user, business

    def reduce_user_business(self, user, business):
        for b in business:
            yield user+':'+str(len(set(b))), set(b)
    
    
    def map_business(self, user_count, business):
        for b in business:
            yield b, user_count

    def combine_by_business(self, business, user_count):
        yield business, user_count


    
    def map_common_user(self, _, user):
        for u_idx in range(len(user)):
            for i_idx in range(u_idx+1, len(user)):
                uu = user[u_idx]
                ui = user[i_idx]
                u1, u_count = uu.split(':')
                i1, i_count = ui.split(':')
                yield (u1, int(u_count), i1, int(i_count)), 1

    def reduce_common_user(self, user_pair, counts):
        yield user_pair, sum(counts)

    

    def reduce_user_pair(self, user_pair, sum_counts):
        common = sum(sum_counts) 
        sim = common/(user_pair[1] + user_pair[3] - common)
        if sim >= 0.5:
        	yield (user_pair[0], user_pair[2]), sim

    
    def steps(self):
        return [MRStep(mapper=self.map_users,
                       reducer=self.combine_user_business),
                MRStep(reducer=self.reduce_user_business),
                MRStep(mapper=self.map_business,
                       reducer=self.combine_by_business),
                MRStep(mapper=self.map_common_user,
                       reducer=self.reduce_common_user),
                MRStep(reducer=self.reduce_user_pair)
        ]

if __name__ == '__main__':
    ReviewWordCount.run()