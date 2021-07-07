#!bin/bash

for i in 1 2 3 4 5 6
do
	python3 general_simple_actor.py --num-requests=16240 --batch-size=256 --num-clients=$i --exactly-once --checkpoint-freq=1000
done