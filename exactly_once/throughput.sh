#!bin/bash

for i in 1 2 3 4 5 6
do
	python3 general_simple_actor.py --num-requests=20000 --num-clients=$i
done