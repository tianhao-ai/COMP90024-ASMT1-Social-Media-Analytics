from mpi4py import MPI
import json
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Load the sal.json file to get a dictionary of all capital cities and their locations
if rank == 0:
    start_time = time.time()
    cities = {}
    with open("sal.json") as f:
        data = json.load(f)
        for key in data:
            location = key
            if data[key]['gcc'] in cities:
                cities[data[key]['gcc']].append(location)
            else:
                cities[data[key]['gcc']]=[]
                cities[data[key]['gcc']].append(location)
else:
    cities = None

# Broadcast the cities dictionary to all processes
cities = comm.bcast(cities, root=0)

# Create a dictionary to store the tweet counts for each author
author_tweet_counts = {}
city_tweet_counts = {}
# Load the tweet.json file and divide the tweets among the processes
with open("twitter-data-small.json") as f:
    tweets = json.load(f)
    chunk_size = len(tweets) // size
    start = rank * chunk_size
    end = start + chunk_size
    my_tweets = tweets[start:end]

# Count the number of tweets for each author in the current process's chunk
for tweet in my_tweets:
    author_id = tweet['data']['author_id']
    if author_id in author_tweet_counts:
        author_tweet_counts[author_id] += 1
    else:
        author_tweet_counts[author_id] = 1

    # Extract the location from the tweet data
    location = tweet['includes']['places'][0]['full_name'].split(',')[0].lower()
    # Check if the location is in one of the capital cities
    for city in cities:
        if location in cities[city]:
            if city in city_tweet_counts:
                city_tweet_counts[city] += 1
            else:
                city_tweet_counts[city] = 1
            break

# Reduce the author_tweet_counts across all processes
all_author_tweet_counts = comm.gather(author_tweet_counts, root=0)
all_city_tweet_counts = comm.gather(city_tweet_counts, root=0)

if rank == 0:
    
    # Sum the tweet counts for each city across all processes
    sum_all_city_tweet_counts = {city: 0 for city in cities}
    for d in all_city_tweet_counts:
        for city, count in d.items():
            if city in sum_all_city_tweet_counts:
                sum_all_city_tweet_counts[city]+=count
            else:
                sum_all_city_tweet_counts[city] = count
    sort_sum_all_city = sorted(sum_all_city_tweet_counts.items(), key=lambda x: x[1], reverse=True)
    
    print("Greater Capital City\tNumber of Tweets Made")
    # Print the tweet counts for each city
    for i in range(len(sort_sum_all_city)):
        city,count = sort_sum_all_city[i]
        print(f"{city}\t\t\t\t{count}")
    
    print("\n")
        
    # Combine the tweet counts for each author across all processes
    final_author_tweet_counts = {}
    for d in all_author_tweet_counts:
        for author_id, count in d.items():
            if author_id in final_author_tweet_counts:
                final_author_tweet_counts[author_id] += count
            else:
                final_author_tweet_counts[author_id] = count

    # Sort the authors by tweet count and print the top 10
    top10 = sorted(final_author_tweet_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    max_author_id_len = max(len(str(author_id)) for author_id, _ in top10)
    # Print the top 10 authors
    print(f"Rank\t{'Author Id':<{max_author_id_len}}\tNumber of Tweets Made")
    for i in range(len(top10)):
        author_id, num_tweets = top10[i]
        print(f"#{i+1}\t{author_id:<{max_author_id_len}}\t\t{num_tweets}")
    print(f"Elapsed time: {time.time() - start_time:.2f} seconds\n")
