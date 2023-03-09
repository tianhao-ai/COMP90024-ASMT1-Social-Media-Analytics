# ccc-assignment1

# Counting tweets using MPI
This program counts the number of tweets made by different author ids in a JSON file using MPI (Message Passing Interface) to distribute the computation across multiple processes.

# Prerequisites
To run this program, you will need:

* Python 3.x
* mpi4py (can be installed using pip)
* Open MPI

You can install Open MPI on macOS using Homebrew:

```
brew install openmpi
```

You can install mpi4py using pip. Open Terminal and run the following command:

```
pip install mpi4py
```

# Usage
To run the program, first clone the repository:

`git clone https://github.com/<username>/<repository>.git`
Replace <username> and <repository> with your GitHub username and the name of your repository.

Then, navigate to the repository directory:

```cd <repository>```
    
Replace <repository> with the name of your repository.

To run the program, use the following command:

```mpiexec -n <num_processes> python count_tweets.py```

Replace <num_processes> with the number of processes you want to use for the computation.

Here's an example of how to run the program with 2 processes:

```mpiexec -n 2 python count_tweets.py```
    
The program will count the number of tweets made by different author ids in the tweets.json file and present the top 10 authors with the most tweets.

# Acknowledgments
This program was created as a project for COMP90024 Cluster and Cloud Computing at University of Melbourne. Special thanks to Prof.Richard Sinnott for their guidance and support.