#!/usr/bin/env python3

import csv

count = 0

with open("telefonbuch.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=";")
    for row in reader:
        if row["recordtype_int"] == "0" and row["commercial"] == "" and row["housenumber"] != "":
            count += 1

print("Nichtkommerzielle Einträge:", count)
