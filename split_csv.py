import csv
import math
import sys

'''
Splits a csv file with n rows into k different files of roughly n/k rows (assumes no header row).

Usage: python split_csv.py input.csv k

'''

input_file = sys.argv[1]
k = int(sys.argv[2])
file_prefix = input_file.split(".")[0]

reader = csv.reader(open(input_file))
lines = list(reader)
n = len(lines)

lines_per_file = int(math.ceil(float(n)/k))

for file_num in range(k):
	new_file = file_prefix + "_" + str(file_num) + ".csv"
	writer = csv.writer(open(new_file, "w+"))

	start = file_num * lines_per_file

	for index in range(start, start + lines_per_file):
		if index < len(lines):
			writer.writerow(lines[index])
		else:
			break


