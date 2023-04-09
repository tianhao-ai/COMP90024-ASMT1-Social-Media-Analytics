from mpi4py import MPI
import json
import time
import sys
import ijson

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


# Count the number of tweets for each author in the current process's chunk
def process_tweets(tweet,author_tweet_counts,city_tweet_counts,author_tweet_city,cities,state):
    # Extract the location from the tweet data
    full_location = tweet['includes']['places'][0]['full_name'].split(',')
    author_id = tweet['data']['author_id']
    # part 2 count author tweet
    if author_id in author_tweet_counts:
        author_tweet_counts[author_id] += 1
    else:
        author_tweet_counts[author_id] = 1
    # Check if the location is in one of the capital cities
    for city in cities:      
    # case 1: location, state || case 2: location, location || case 3: location || case 4: location,state(9oter)
        if (full_location[0].strip().lower() in cities[city] and full_location[-1].strip() in state[city]) or (full_location[-1].strip().lower() in cities[city]) or (full_location[0].strip().lower() in cities[city] and state[city]=='Other Territory'):
            # part 1 count city 
            if city in city_tweet_counts:
                city_tweet_counts[city] += 1
            else:
                city_tweet_counts[city] = 1
            # part 3 
            if author_id in author_tweet_city:
                if city in author_tweet_city[author_id]:
                    author_tweet_city[author_id][city] += 1
                else:
                    author_tweet_city[author_id][city] = 1
            else:
                author_tweet_city[author_id] = {}
                author_tweet_city[author_id][city] = 1
    return author_tweet_counts,city_tweet_counts,author_tweet_city

# Broadcast the cities dictionary to all processes
def allocate_gather_core(filename,cities,state):
    # Monitor the time consumption of each process 
    process_search_time = 0 
    # Create a dictionary to store the tweet counts for each author
    # Load the tweet.json file and divide the tweets among the processes
    author_tweet_counts = {}
    city_tweet_counts = {}
    author_tweet_city = {}
    if rank == 0:
        with open(filename, 'r') as f:
            ts_loop_json_item_start = MPI.Wtime()
            tweets = ijson.items(f, 'item')
            tweet_size = 0
            chunk_size = 0
            send_info_time = 0
            # Reading data in a memory efficient way
            for index, tweet in enumerate(tweets):
                tweet_size += 1
                target_rank = index % size # determine this tweet is responsible for which process
                if target_rank == 0:
                    current_tweet_start_time = MPI.Wtime()
                    chunk_size+=1     
                    author_tweet_counts,city_tweet_counts,author_tweet_city = process_tweets(tweet,
                                                                                            author_tweet_counts,
                                                                                            city_tweet_counts,
                                                                                            author_tweet_city,
                                                                                            cities,
                                                                                             state)
                    process_search_time += (MPI.Wtime()-current_tweet_start_time)
                else:
                    send_info_time_start = MPI.Wtime()
                    comm.send(tweet, dest=target_rank, tag=1) # Send the tweet to the target process
                    send_info_time += (MPI.Wtime()-send_info_time_start)
            ts_loop_json_item_time = MPI.Wtime() - ts_loop_json_item_start
            for proc in range(1, size):
                comm.send(None, dest=proc, tag=1) # Send the trigger to each process to stop the while loop
    else:
        chunk_size = 0
        while True:
            tweet = comm.recv(source=0, tag=1)
            if tweet is None:
                break
            current_tweet_start_time = MPI.Wtime()
            author_tweet_counts,city_tweet_counts,author_tweet_city = process_tweets(tweet,author_tweet_counts,city_tweet_counts,author_tweet_city,cities,state)
            chunk_size+=1
            process_search_time += (MPI.Wtime()-current_tweet_start_time)
            
    process_chunk_size = chunk_size

    # Reduce the author_tweet_counts across all processes
    all_author_tweet_counts = comm.gather(author_tweet_counts, root=0)
    all_city_tweet_counts = comm.gather(city_tweet_counts, root=0)
    all_author_tweet_city = comm.gather(author_tweet_city, root=0)
    all_process_search_time = comm.gather(process_search_time, root=0)
    all_process_chunk_size = comm.gather(process_chunk_size, root=0)
    # Now we using mpi finalizing all the needed dictionary and start printing out the results
    return all_author_tweet_counts,all_city_tweet_counts,all_author_tweet_city,all_process_search_time,all_process_chunk_size,ts_loop_json_item_time,tweet_size


# this function is to print the rank of each tweet made each city ranked by the number of tweet made
# The param include the gathered data of all city counts, which are several dictionary
def count_tweet_city(all_city_tweet_counts,cities):
    sum_all_city_tweet_counts = {city: 0 for city in cities}
    for d in all_city_tweet_counts:
        for city, count in d.items():
            if city in sum_all_city_tweet_counts:
                sum_all_city_tweet_counts[city]+=count
            else:
                sum_all_city_tweet_counts[city] = count
    sort_sum_all_city = sorted(sum_all_city_tweet_counts.items(), key=lambda x: x[1], reverse=True)

    print("Greater Capital City\tNumber of Tweets Made")
    print('--------------------------------------------')
        # Print the tweet counts for each city
    for i in range(len(sort_sum_all_city)):
        city,count = sort_sum_all_city[i]
        print(f"{city}\t\t\t\t{count}")

    print("\n")


def count_tweet_person(all_author_tweet_counts):
    final_author_tweet_counts = {}
    for d in all_author_tweet_counts:
        for author_id, count in d.items():
            if author_id in final_author_tweet_counts:
                final_author_tweet_counts[author_id] += count
            else:
                final_author_tweet_counts[author_id] = count

        # Sort the authors by tweet count and print the top 10
    sorted_counts = sorted(final_author_tweet_counts.items(), key=lambda x: x[1], reverse=True)
        # Initialize variables to store top authors and rank
    top_authors = []
    places = 1
    prev_count = sorted_counts[0][1]

        # Iterate through sorted counts and add authors to top_authors until we have 10 unique ranks
    for author_id, count in sorted_counts:
        if count < prev_count:
            places += 1
        if places <= 10:
            top_authors.append((author_id, count, places))
        else:
            break
        prev_count = count

    max_author_id_len = max(len(str(author_id)) for author_id, _, _ in top_authors)

        # Print the top authors
    print(f"Rank\t{'Author Id':<{max_author_id_len}}\tNumber of Tweets Made")
    print('------------------------------------------------------------------') 
    for author_id, num_tweets, places in top_authors:
        print(f"#{places}\t{author_id:<{max_author_id_len}}\t\t{num_tweets}")

    print("\n")

# Combine the tweet counts for each author across tweet in all city in all processes
# Function for part 3
def author_city_rank(all_author_tweet_city,cities):
    final_author_tweet_city = {}
    for d in all_author_tweet_city:
        for key, nested_dict in d.items():
            if key not in final_author_tweet_city:
                final_author_tweet_city[key] = {}
            for nested_key, value in nested_dict.items():
                if nested_key in final_author_tweet_city[key]:
                    final_author_tweet_city[key][nested_key] += value
                else:
                    final_author_tweet_city[key][nested_key] = value

        # Create a list of tuples with the required information
    authors_data = []
    for author_id, cities in final_author_tweet_city.items():
        unique_cities = len(cities)
        total_tweets = sum(cities.values())
        authors_data.append((author_id, unique_cities, total_tweets, cities))

        # Sort the list based on the number of unique city locations and total tweets in descending order
    authors_data.sort(key=lambda x: (-x[1], -x[2]))
        
    places = 1
    top10 = []
    prev_unique_cities = authors_data[0][1]
    prev_total_tweets = authors_data[0][2]
    for author in authors_data:
        curr_unique_cities = author[1]
        curr_total_tweets = author[2]
        if curr_unique_cities < prev_unique_cities:
            places += 1
        elif curr_unique_cities == prev_unique_cities:
            if curr_total_tweets < prev_total_tweets:
                places += 1         
        if places <= 10:
            top10.append((author[0],author[1],author[2],author[3],places))
        else:
            break
        prev_unique_cities = curr_unique_cities
        prev_total_tweets = curr_total_tweets

        # Print header
    print(f"{'Rank':<5}{'Author Id':<20}{'Number of Unique City Locations and #Tweets':<50}")
    print('---------------------------------------------------------------------------------')

        # Print the top 10 output
    for author_id, unique_cities, total_tweets, cities,place in top10:
        city_breakdown = ', '.join([f"{tweet_count}{city[1:]}" for city, tweet_count in cities.items()])
        print(f"#{place:<4}{author_id:<20}{unique_cities} (#{total_tweets} tweets - {city_breakdown})")



def main(filename):
    # Load the sal.json file to get a dictionary of all capital cities and their locations
    if rank == 0:
        start_time = time.time()
        ts_start_time_0 = MPI.Wtime()
        state = {
        '1gsyd': 'New South Wales',
        '2gmel': 'Victoria',
        '3gbri': 'Queensland',
        '4gade': 'South Australia',
        '5gper': 'Western Australia',
        '6ghob': 'Tasmania',
        '7gdar': 'Northern Territory',
        '8acte': 'Australian Capital Territory',
        '9oter': 'Other Territory'}
        aus_cap_city = ['1gsyd','2gmel','3gbri','4gade','5gper','6ghob','7gdar', '8acte', '9oter']
        cities = {}
        with open("sal.json") as f:
            data = json.load(f)
            for key in data:
                location = key
                if data[key]['gcc'] in cities:
                    cities[data[key]['gcc']].append(location)
                elif data[key]['gcc'] not in cities and data[key]['gcc'] in aus_cap_city:
                    cities[data[key]['gcc']]=[]
                    cities[data[key]['gcc']].append(location)
        ts_0 = MPI.Wtime() - ts_start_time_0
        
    else:
        cities = None
        state = None 

    # Broadcast the cities dictionary to all processes
    cities = comm.bcast(cities, root=0)
    state = comm.bcast(state, root=0)

    # Allocate the data into cores and return and gather the results from individual core which used to provide the results
    process_search_time = 0 
    # Create a dictionary to store the tweet counts for each author
    # Load the tweet.json file and divide the tweets among the processes
    author_tweet_counts = {}
    city_tweet_counts = {}
    author_tweet_city = {}
    if rank == 0:
        with open(filename, 'r') as f:
            ts_loop_json_item_start = MPI.Wtime()
            tweets = ijson.items(f, 'item')
            tweet_size = 0
            chunk_size = 0
            send_info_time = 0
            for index, tweet in enumerate(tweets):
                tweet_size += 1
                target_rank = index % size # determine this tweet is responsible for which process
                if target_rank == 0:
                    current_tweet_start_time = MPI.Wtime()
                    chunk_size+=1     
                    author_tweet_counts,city_tweet_counts,author_tweet_city = process_tweets(tweet,
                                                                                            author_tweet_counts,
                                                                                            city_tweet_counts,
                                                                                            author_tweet_city,
                                                                                            cities,
                                                                                             state)
                    process_search_time += (MPI.Wtime()-current_tweet_start_time)
                else:
                    send_info_time_start = MPI.Wtime()
                    comm.send(tweet, dest=target_rank, tag=1) # Send the tweet to the target process
                    send_info_time += (MPI.Wtime()-send_info_time_start)
            ts_loop_json_item_time = MPI.Wtime() - ts_loop_json_item_start
            for proc in range(1, size):
                comm.send(None, dest=proc, tag=1) # Send the trigger to each process to stop the while loop
    else:
        chunk_size = 0
        while True:
            tweet = comm.recv(source=0, tag=1)
            if tweet is None:
                break
            current_tweet_start_time = MPI.Wtime()
            author_tweet_counts,city_tweet_counts,author_tweet_city = process_tweets(tweet,author_tweet_counts,city_tweet_counts,author_tweet_city,cities,state)
            chunk_size+=1
            process_search_time += (MPI.Wtime()-current_tweet_start_time)
            
    process_chunk_size = chunk_size

    # Reduce the author_tweet_counts across all processes
    all_author_tweet_counts = comm.gather(author_tweet_counts, root=0)
    all_city_tweet_counts = comm.gather(city_tweet_counts, root=0)
    all_author_tweet_city = comm.gather(author_tweet_city, root=0)
    all_process_search_time = comm.gather(process_search_time, root=0)
    all_process_chunk_size = comm.gather(process_chunk_size, root=0)


    if rank == 0:
        ts_part_1_start = MPI.Wtime() 
   
    # function for print the result of part 1
        count_tweet_person(all_author_tweet_counts)

    # function for print the result of part 2
        count_tweet_city(all_city_tweet_counts,cities)
    # function for print the result of part 3
        author_city_rank(all_author_tweet_city,cities)
        
        '''
        The part below is focus on print the time consumption of each part of the code
        '''
            
        ts_part_1 = MPI.Wtime() - ts_part_1_start

        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print('\n')
        for count,value in enumerate(all_process_search_time):
            if count == 0:
                ts_loop_json_item_time -= value
            print(f"Process {count} finished search {all_process_chunk_size[count]} tweets in: {value:.5f} seconds\n")
            print(f"Process {count} search {all_process_chunk_size[count]} tweets in rate: {all_process_chunk_size[count]/value:.5f} tweets/second\n")
        print(f"Time spent in the sequential portion on load data: {ts_0:.5f} seconds\n")
        print(f"Time spent in the sequential portion on loop through json object: {ts_loop_json_item_time:.5f} seconds\n")
        print(f"Time spent in the sequential portion on output result: {ts_part_1:.5f} seconds\n")
        print(f"Time spent in the sequential portion: {ts_part_1+ts_0+ts_loop_json_item_time:.5f} seconds\n")
        print(f"Note: process 0 cost {send_info_time:.5f} seconds to send information to other process in sequential portion\n")
        print(f"Task finished process {tweet_size} tweets\n")
        print(f"Task Elapsed time: {elapsed_time:.5f} seconds\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mpirun -n n python file.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    main(filename)
