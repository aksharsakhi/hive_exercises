#!/bin/bash

echo "Downloading datasets for Question 4 & 5..."

# Move to inputs directory
cd "$(dirname "$0")"

# 1. MovieLens Dataset (Small)
echo "Downloading MovieLens dataset..."
curl -O https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -o ml-latest-small.zip
mv ml-latest-small/movies.csv .
mv ml-latest-small/ratings.csv .
rm -rf ml-latest-small ml-latest-small.zip

# 2. UCI Online Retail Dataset
echo "Downloading UCI Online Retail dataset..."
# The UCI dataset is often an excel file, but kaggle/github mirrors provide CSV.
# Using a raw github URL for the CSV version to avoid parsing Excel locally
curl -L -o OnlineRetail.csv https://raw.githubusercontent.com/jadianes/data-science-your-way/master/02-understanding-data/data/online_retail.csv

# 3. Dummy Customer Details for Q5 part xi
echo "Creating dummy CustomerDetails.csv..."
cat << EOF > CustomerDetails.csv
12346,Alice Smith,London,Premium
12347,Bob Johnson,Paris,Basic
12348,Charlie Brown,Berlin,Premium
12349,David Jones,Madrid,Standard
12350,Eve Davis,Rome,Premium
17850,Frank Miller,London,Premium
13047,Grace Taylor,Manchester,Basic
12583,Heidi Wilson,Paris,Premium
14688,Ivan Moore,London,Standard
15311,Judy Clark,Berlin,Premium
14527,Kevin King,London,Standard
16029,Laura White,Paris,Basic
17420,Mike Scott,Rome,Premium
12431,Nancy Green,Madrid,Premium
12791,Oscar Adams,Berlin,Basic
13237,Pat Baker,London,Premium
15291,Quinn Hall,Paris,Standard
13767,Rachel Allen,Manchester,Premium
14110,Steve Young,Rome,Standard
14646,Tom Wright,Berlin,Premium
EOF

echo "All datasets prepared successfully!"
