import sys
import datetime
from pathlib import Path
import statistics

class drutils:
    'Class with utils to make data recording (dr) easier'

    #Class variables
    _outputModeList = ["f","s","b","n"] # file, screen, both, none (quick way to turn logging off without removing code)
    _dataFolderRelativePath = Path("../data/") #pathlib translates / as needed for linux(posix) or Windows
    if _dataFolderRelativePath.is_dir() == False:
        print("ERROR: " + str(_dataFolderRelativePath) + "doesn't exist, please create one!")

    #Class methods
    @staticmethod
    def createTS_filename(prefix):
        filename = drutils._dataFolderRelativePath / str(prefix + "{:%Y-%m-%d_%H%M%S}".format(datetime.datetime.now()) + ".txt")
        return filename
    
    @staticmethod
    def createCSV_filename(prefix):
        filename = drutils._dataFolderRelativePath / str(prefix + ".csv")
        return filename

    #constructor
    def __init__(self, prefix, mode, filetype = "txt"):
        if filetype == "txt":
            self.filename = drutils.createTS_filename(prefix)
        elif filetype == "csv":
            self.filename = drutils.createCSV_filename(prefix)
        else:
            print("WARNING: txt or csv file type not correctly specified to drutils.")
            return
        if mode in(drutils._outputModeList):
            self.mode = mode
            if self.mode == "n": # Don't get a file handle
                print("WARNING: Data recording mode set to 'n', no files logging will be done")
                return
            if self.mode == "s":
                print("Recording data to screen only")
        else:
            print("Error creating DataCollection file handle. Defaulting to (s)creen.", sys.stderr)
            self.mode = "s"
        if self.mode == "f" or self.mode == "b":
            try:
                print("Recording data in file: ", self.filename)
                self.fhandle = open(self.filename, "a") #append, default to text
            except:
                print("Error creating DataCollection file handle.", sys.stderr)
        return
    def rcrd_data(self, string = "\n"):
        if self.mode == "n":
            return
        if self.mode == "s" or self.mode == "b":
            print(string)
        if self.mode == "f" or self.mode == "b":
            self.fhandle.write(string+"\n")
        return
    def close(self):
        if self.mode == "n":
            return
        if self.mode == "f" or self.mode == "b":
            print("Closing file: ", self.filename)
            self.fhandle.close()
    def log_csv2file(self, listParams, checkNumber = 0):
        if self.mode == "n":
            return
        if not(self.mode == "f" or self.mode == "b"):
            print("ERROR: Can't log_csv to a file if you didn't open a file")
            return False
        if checkNumber > 0:
            if len(listParams) != checkNumber:
                print("ERROR: Passing incorrect number of paramesters to log_csv")
                return False
        #build string to write
        csvString = str(listParams)[1:-1].replace("'","") # Yeah, it's that easy
                                                          # Replace will get rid of ''s on strings making csv easier to deal with
        try:
            self.fhandle.write(csvString + "\n")
        except:
            print("ERROR writing to csv file.", sys.stderr)   
        return

