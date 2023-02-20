#!/usr/bin/env python
# coding: utf-8

# In[1]:


import chess.pgn

# define the two PGN files to merge
pgn_files = ["C:/Users/sebas/Downloads/chess_com_games_2023-02-202.pgn", 
             "C:/Users/sebas/Downloads/chess_com_games_2023-02-20.pgn"]

# create a new PGN file to store the merged games
merged_pgn_file = open("merged.pgn", "w")

# loop through each PGN file and add the games to the merged file
for pgn_file in pgn_files:
    with open(pgn_file) as f:
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            headers = game.headers
            # write the game to the merged PGN file
            merged_pgn_file.write(str(game) + "\n\n")


# In[2]:


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from chess.pgn import read_game
from datetime import datetime
import csv

# Define the headers we want to extract
headers = ["Event", "Site", "Date", "Round", "White", "Black", "Result", "WhiteElo", "BlackElo", "Moves"]

# Define a function to extract the headers from a game
def extract_headers(game):
    header_data = {}
    for header in headers:
        header_data[header] = game.headers.get(header, "")
    header_data["Date"] = datetime.strptime(header_data["Date"], "%Y.%m.%d")
    return header_data

# Define a function to extract the moves from a game
def extract_moves(game):
    moves = []
    node = game
    while node.variations:
        next_node = node.variations[0]
        moves.append(str(node.board().san(next_node.move)))
        node = next_node
    return " ".join(moves)

# Open the merged PGN file
with open("merged.pgn") as f:
    games = []
    while True:
        game = read_game(f)
        if game is None:
            break
        headers = extract_headers(game)
        moves = extract_moves(game)
        headers["Moves"] = moves
        games.append(headers)

# Convert the list of games to a Pandas DataFrame
df = pd.DataFrame(games)

# Export the DataFrame to CSV
df.to_csv("merged.csv", index=False, sep="|", quoting=csv.QUOTE_NONNUMERIC)

# Convert the Pandas DataFrame to a PyArrow Table
table = pa.Table.from_pandas(df)

# Export the PyArrow Table to Parquet
pq.write_table(table, "merged.parquet")


# In[3]:


#Final file
df = pd.read_parquet("merged.parquet")


# In[4]:


df.head()

