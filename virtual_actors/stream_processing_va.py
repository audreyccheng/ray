import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
import ray
import virtual_actors_master as va
import wikipedia

parser = argparse.ArgumentParser()
parser.add_argument(
	"--num-mappers", help="number of mapper actors used", default=1, type=int)
parser.add_argument(
	"--num-reducers",
	help="number of reducer actors used",
	default=2,
	type=int)


# @ray.remote
class Mapper(object):
	def __init__(self):
		self.title_stream = None
		self.num_articles_processed = 0
		self.articles = []
		self.word_counts = []

	def setup(self, title_stream):
		print("mapper setup")
		self.title_stream = title_stream

	def get_new_article(self):
		# Get the next wikipedia article.
		article = wikipedia.page(self.title_stream.next()).content
		# Count the words and store the result.
		self.word_counts.append(Counter(article.split(" ")))
		self.num_articles_processed += 1

	def get_range(self, article_index, keys):
		print("mapper")
		# Process more articles if this Mapper hasn't processed enough yet.
		while self.num_articles_processed < article_index + 1:
			print("get new article")
			self.get_new_article()
		# Return the word counts from within a given character range.
		print("mapper returns")
		print(self)
		return [(k, v) for k, v in self.word_counts[article_index].items()
				if len(k) >= 1 and k[0] >= keys[0] and k[0] <= keys[1]]

# @ray.remote
class Reducer(object):
	def __init__(self):
		# pass
		self.mappers = None
		self.keys = None

	def setup(self, keys, *mappers):
		print("reducer setup")
		self.mappers = mappers
		self.keys = keys

	def next_reduce_result(self, article_index):
		print("reducer")
		word_count_sum = defaultdict(lambda: 0)
		# Get the word counts for this Reducer's keys from all of the Mappers
		# and aggregate the results.
		count_ids = [
			mapper.send_request.remote("get_range", article_index, self.keys)
			for mapper in self.mappers
		]
		print("count ids")
		print(count_ids)
		for count_id in count_ids:
			print(count_id)
			for k, v in ray.get(count_id):
				word_count_sum[k] += v
		# while len(count_ids):
		# 	done_id, count_ids = ray.wait(count_ids)
		# 	for k, v in ray.get(done_id[0]):
		# 		word_count_sum[k] += v
		print("reducer done")
		return word_count_sum


class Stream(object):
	def __init__(self, elements):
		self.elements = elements

	def next(self):
		i = np.random.randint(0, len(self.elements))
		return self.elements[i]


if __name__ == "__main__":
	args = parser.parse_args()

	ray.init()

	# Create one streaming source of articles per mapper.
	directory = os.path.dirname(os.path.realpath(__file__))
	streams = []
	for _ in range(args.num_mappers):
		with open(os.path.join(directory, "articles.txt")) as f:
			streams.append(Stream([line.strip() for line in f.readlines()]))

	# Partition the keys among the reducers.
	chunks = np.array_split([chr(i)
							 for i in range(ord("a"),
											ord("z") + 1)], args.num_reducers)
	keys = [[chunk[0], chunk[-1]] for chunk in chunks]

	virtual_actor_group = va.VirtualActorGroup.options(
		name="VirtualActorGroup", lifetime="detached").remote(2)

	# Create a number of mappers.
	# mappers = [Mapper.remote(stream) for stream in streams]
	mappers = [va.Client.options(max_concurrency=10).remote(virtual_actor_group, Mapper) for _ in streams]
	ray.get([client.send_request.remote("setup", stream) for client, stream in zip(mappers, streams)])
	print(mappers)

	# Create a number of reduces, each responsible for a different range of
	# keys. This gives each Reducer actor a handle to each Mapper actor.
	# reducers = [Reducer.remote(key, *mappers) for key in keys]
	reducers = [va.Client.options(max_concurrency=10).remote(virtual_actor_group, Reducer) for _ in keys]
	ray.get([client.send_request.remote("setup", key, *mappers) for client, key in zip(reducers, keys)])
	# print(reducers)

	article_index = 0
	while article_index <= 0:
		print("article index = {}".format(article_index))
		wordcounts = {}
		counts = ray.get([
			reducer.send_request.remote("next_reduce_result", article_index)
			for reducer in reducers
		])
		for count in counts:
			wordcounts.update(count)
		most_frequent_words = heapq.nlargest(
			10, wordcounts, key=wordcounts.get)
		for word in most_frequent_words:
			print("  ", word, wordcounts[word])
		article_index += 1

	ray.get(virtual_actor_group.close.remote())



