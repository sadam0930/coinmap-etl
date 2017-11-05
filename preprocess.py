import csv
with open("bitvenues.csv", "rb") as input, open("bitvenues2.csv", "wb") as output:
    w = csv.writer(output)
    for record in csv.reader(input):
        w.writerow(tuple(s.replace('\n', ' ') for s in record))