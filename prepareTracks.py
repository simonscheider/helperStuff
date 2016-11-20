#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      simon
#
# Created:     20/11/2016
# Copyright:   (c) simon 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import numpy as np
import pandas as pd
import csv

class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

def file_to_str(fn, dt):
    """
    Loads the content of a text file into a DataFrame, where datetimes in columns dt are converted to timestamps
    @return a string
    """
    content = pd.read_csv(fn, index_col=False, lineterminator='\r', header = None, parse_dates=dt, sep='\t')
    #with open(fn, 'r') as f:
    #    content=f.read()
    return content

rows = Queue()

def dequeueRows(purpose, csv_out):

    i = 0
    while i < rows.size():
        i+=1
        row = rows.dequeue()
        csv_out.writerow((row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], None, None,purpose, None,None))




def main():
    track =  file_to_str('tracktest.csv', [2])
    journey =  file_to_str('journeytest.csv',[3,5])
    track.sort([0, 1, 2], ascending=True)
    #print track
    #track.

    with open('trackout.csv','wb') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['person','track', 'datetime', 'X', 'Y', '6', '7', '8', '9', 'trip', 'mode', 'purpose', 'start', 'end'])
        tracknr = ''
        purpose = ''
        origin = ''
        for row in track.itertuples():
            print row
            #Select the journeys of the particular user
            journeys =  journey.loc[journey[0]==row[1]]
            #select first matching journey
            t = True
            for j in journeys.itertuples():
                #print j
                if row[3]>=j[4] and row[3]<=j[6]:
                    purpose = j[8].split("-")[0]
                    origin = j[8].split("-")[1]
                    #if tracknr == row[2]:
                    dequeueRows(purpose,csv_out)
                    print(str(row[3]) +' is in interval '+str(j[4]) +str(j[6]))
                    csv_out.writerow((row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], j[2], j[3],j[8], j[4],j[6]))
                    t = False
                    break
            if t:
                rows.enqueue(row)
            if tracknr == row[2]:
                dequeueRows(origin,csv_out)
            tracknr = row[2]

            #for row2 in journey
            #if row[3] >
    out.close()

if __name__ == '__main__':
    main()
