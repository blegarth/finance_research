import os
import re
import pprint as pretty
import csv
import time
import logging
from functools import reduce
from datetime import datetime
from striprtf.striprtf import rtf_to_text
from textstat.textstat import textstat


RA_Folder = r'/Users/steve/Desktop/steve_research'
subDocPath = r'/Users/steve/Desktop/steve_research/subDocFolder'
causalWordsFile = r'/Users/steve/Desktop/steve_research/CausalWordDictionary.txt'
transcriptFolder = r'/Users/steve/Desktop/steve_research/transcripts'
rtfPath = r'/Users/steve/Desktop/steve_research/FactivaDocs'
excelFile = r'/Users/steve/Desktop/steve_research/MasterData.csv'
tickerFolder = r'/Users/steve/Desktop/steve_research/tickerFolder'
compPath = r'/Users/steve/Desktop/steve_research/compFolder'
stockPath = r'/Users/steve/Desktop/steve_research/stockFolder'

logging.basicConfig(level=logging.DEBUG)


def splitTextFile():
    """Takes a STARTER txt file and processes it into NUM individual files."""
    fileNum = 1
    for transcriptFile in os.listdir(transcriptFolder):
        if transcriptFile.startswith('.'):
            continue
        else:
            # opens file to be sliced as "master"
            with open('{}/{}'.format(transcriptFolder, transcriptFile), 'r') as masterFile:
                for i in range(1, 101):
                    fileNum += 1
                    # opens file to be written into as "smaller"
                    with open('{}/{}.txt'.format(subDocPath, str(fileNum)), 'w') as smaller:
                        for line in masterFile:
                            if not line.isspace() and line.strip() != 'HD':
                                if 'Document FND' not in line:
                                    smaller.write(line)
                                else:
                                    smaller.write(line)
                                    break
                logging.info('Finished writing 100 individual earnings calls')
    logging.info("Number of files created in directory 'subDocFolder': " +
                 str(totalNumOfFiles(subDocPath)) + '\n')


def changeRtfFile():
    """Converts files from rtf to text"""
    for transcriptFile in os.listdir(rtfPath):
        if transcriptFile.startswith('.'):
            continue
        else:
            # opens file to be written into as "smaller"
            with open('{}/{}.txt'.format(transcriptFolder, transcriptFile[:-4]), 'w') as smaller:
                data = open('{}/{}'.format(rtfPath, transcriptFile), 'r')
                newData = rtf_to_text(data.read())
                smaller.write(newData)

    logging.info("Number of files created in directory 'transcriptFolder': " +
                 str(totalNumOfFiles(transcriptFolder)) + '\n')


def totalNumOfFiles(directory):
    """Returns the number of files in a directory"""
    return len([item for item in os.listdir(directory) if os.path.isfile(os.path.join(directory, item)) and not item.startswith('.')])


def renameFiles():
    """Renames files."""

    for fileName in os.listdir(subDocPath):
        if fileName.startswith('.'):
            continue
        else:
            with open('{}/{}'.format(subDocPath, fileName), 'r') as updated:
                firstLine = updated.readline().strip()
                try:
                    os.rename('{}/{}'.format(subDocPath, fileName),
                              '{}/{}.txt'.format(subDocPath, firstLine))
                except FileNotFoundError:
                    withoutSlash = [x for x in list(firstLine) if x != '/']
                    os.rename('{}/{}'.format(subDocPath, fileName),
                              '{}/{}.txt'.format(subDocPath, "".join(withoutSlash)))


def wordsToList():
    """returns causal words as list"""

    words = []
    with open(causalWordsFile, 'r') as f:
        for line in f:
            # if applicable, strips new line character and appends word to "words" list
            if line[-1] == '\n':
                words.append(line[:-1])
            else:
                words.append(line)
    logging.info("Successfully created wordDict.\n")
    return words


def initializeDict():
    """creates new wordDict, sets all values to 0"""

    wordDict = {k: 0 for k in wordList}
    if len(wordDict) == len(wordList):
        return wordDict
    else:
        raise ValueError("Check your code (initializeDict function).")


def countWordFrequency(tickerDict):
    """recursively calls the return value of initializeDict() and tracks word frequency"""
    index = 0
    for uncountedFile in os.listdir(subDocPath):
        if uncountedFile.startswith('.'):
            continue
        else:
            index += 1
            newDict = initializeDict()
            with open('{}/{}'.format(subDocPath, uncountedFile), 'r') as f:
                newDict['Index'] = index
                newDict['Document Name'] = uncountedFile[:-4]
                newDict['Report Period & Year'] = titleDateSlicer(
                    newDict['Document Name'])
                newDict['Gunning-Fog'] = textstat.gunning_fog(str(f))
                newDict['Company Name'] = titleCompanySlicer(
                    newDict['Document Name'])
                newDict['Ticker'] = getTickerName(
                    newDict['Company Name'], tickerDict)

                for lineNum, line in enumerate(f):
                    if lineNum == 1:
                        if line.strip() != 'WC':
                            newDict['Word Count'] = line.rstrip()[:-5]
                        else:
                            newDict['Word Count'] = line.strip()[:-5]
                    elif lineNum == 2:
                        newDict['Date'] = datetime.strftime(datetime.strptime(line.strip(), '%d %B %Y'), '%Y%m%d')
                    else:
                        for word in wordList:
                            if word[-1] == "*":
                                newDict[word.lower(
                                )] += len(re.findall(word[:-1] + r'\S*', line.lower()))
                            else:
                                newDict[word.lower(
                                )] += len(re.findall(r'\b' + word + r'\b', line.lower()))
            newDict['Total Instances'] = sum(
                [x for x in newDict.values() if isinstance(x, int)]) - newDict['Index']

            sevDRet = getSevenDayReturn(newDict['Ticker'], newDict['Date'])
            newDict['sevdayret'] = sevDRet
            compD = getCompData(newDict['Ticker'])
            if compD == None:
                compD = [None, None, None, None, None]
            newDict['datacqtr'] = compD[0]
            newDict['atq'] = compD[1]
            newDict['dlttq'] = compD[2]
            newDict['niq'] = compD[3]
            newDict['revtq'] = compD[4]

            logging.debug('Finished processing {}'.format(
                newDict['Document Name']))

            master.append(newDict)

    writeToCSV(master)
    print('\n' + "Excel File Written.")


def connectCompustat():
    """Makes all the compustat data into a dictionary"""
    someDict = {}
    for ufile in os.listdir(compPath):
        if ufile.startswith('.'):
            continue
        else:
            with open('{}/{}'.format(compPath, ufile), 'r') as f:
                csv_reader = csv.reader(f, delimiter=',')
                line_count = 1
                for row in csv_reader:
                    compMaster[row[0]] = row[1:]

def connectStockPrice():
    """Connects all the stock tickers to their prices"""
    someDict = {}
    for ufile in os.listdir(stockPath):
        if ufile.startswith('.'):
            continue
        else:
            with open('{}/{}'.format(stockPath, ufile), 'r') as f:
                csv_reader = csv.reader(f, delimiter=',')
                line_count = 1
                for row in csv_reader:
                    if row[2] is in stockMaster.keys():
                        stockMaster[row[2]][row[1]] = row[5]
                    else:
                        stockMaster[row[2]] = {}
                        stockMaster[row[2]][row[1]] = row[5]


def titleDateSlicer(title):
    """Takes the earnings call transcript and extracts the date"""
    if title[:5] == "Event":
        title = title[15:]

    if title[:7] == "Q3 & 9M":
        return title[:12]
    elif title[:1] == "Q":
        return title[:7]
    elif title[:4] == "Full" or title[:4] == "Half":
        return title[:14]
    elif title[:4] == "Nine":
        return title[:16]
    elif title[:7] == "Interim":
        return title[:12]
    else:
        return "Not an Earnings Transcript"


def titleCompanySlicer(title):
    """Takes the earnings call transcript and extracts the company name"""
    if title[:1] == "Q":
        return title[8:-23]
    elif title[:5] == "Event":
        return title[23:-23]
    elif title[:4] == "Full" or title[:4] == "Half":
        return title[15:-23]
    elif title[:4] == "Nine":
        return title[17:-23]

    elif title.find('at'):
        return title[:title.find('at')]
    else:
        return "N/A"


def makeTickerDict():
    """Creates a dictionary of all the tickers in the data set"""
    tickerDict = {}

    for tickerFile in os.listdir(tickerFolder):
        if tickerFile.startswith('.'):
            continue
        else:
            # opens file to be sliced as "master"
            with open('{}/{}'.format(tickerFolder, tickerFile), 'r') as ticker_data:
                t_reader = csv.reader(ticker_data, delimiter='\t')
                for tick in t_reader:
                    tempVal = tick[1].replace("'", "").replace(
                        ".", "").replace(" ", "").lower()
                    tickerDict[tempVal] = tick[0]

    return tickerDict


def getTickerName(title, tickerDict):
    """Gets the ticker name from the company name"""
    tempName = title.replace("'", "").replace(
        ".", "").replace(" ", "").lower()
    return tickerDict.get(tempName)


def getCompData(title):
    """Returns the compustat data based on the company name"""
    return compMaster.get(title)

def getSevenDayReturn(title, curDate):
    """Returns the seven day return based on a stock ticker and the current date"""
    if stockMaster.get(title) == None:
        if stockMaster.get(title).get(curDate):
            return stockMaster.get(title).get(curDate)
    return None


def writeToCSV(data_to_write):
    """Creates the large data file with all the information that has been collected"""
    with open(excelFile, 'w') as f:
        wordList.insert(0, 'Index')
        wordList.insert(1, 'Document Name')
        wordList.insert(2, 'Report Period & Year')
        wordList.insert(3, 'Date')
        wordList.insert(4, 'Company Name')
        wordList.insert(5, 'Ticker')
        wordList.append('Word Count')
        wordList.append('Gunning-Fog')
        wordList.append('Total Instances')
        wordList.append('sevdayret')
        wordList.append('datacqtr')
        wordList.append('atq')
        wordList.append('dlttq')
        wordList.append('niq')
        wordList.append('revtq')
        writer = csv.DictWriter(f, fieldnames=wordList)
        writer.writeheader()
        for data in data_to_write:
            writer.writerow(data)


def setupFiles():
    changeRtfFile()

    splitTextFile()

    renameFiles()


def main():
    tickerDict = makeTickerDict()
    connectCompustat()
    logging.info('Connected Compustat')
    connectStockPrice()
    logging.info('Connected Stock Prices')

    start = time.time()

    setupFiles()

    countWordFrequency(tickerDict)

    end = time.time()
    print("\nExecution time: " + str(end-start))


if __name__ == "__main__":
    wordList = wordsToList()
    master = []
    compMaster = {}
    stockMaster = {}

    main()
