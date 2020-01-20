import sys
import datetime

class drutils:
    'Class with utils for to make recording data easier'

    #Class variables
    _outputModeList = ["f","s","b"] # file, screen both

    #Class methods
    @staticmethod
    def createTS_filename(prefix):
        filename = prefix+"{:%Y-%m-%d_%H%M%S}".format(datetime.datetime.now())+".txt"
        return filename

    #constructor
    def __init__(self, prefix, mode):
        self.filename = drutils.createTS_filename(prefix)
        if mode in(drutils._outputModeList):
            self.mode = mode
            if self.mode == "s":
                print("Recording data to screen only")
        else:
            print("Error creating DataCollection file handle. Defaulting to (s)creen.", sys.stderr)
            self.mode = "s"
        if self.mode == "f" or self.mode == "b":
            try:
                print("Recording data in file: ", self.filename)
                self.fhandle = open(self.filename, "at") #append, text
            except:
                print("Error creating DataCollection file handle.", sys.stderr)
        return
    def rcrd_data(self, string = "\n"):
        if self.mode == "s" or self.mode == "b":
            print(string)
        if self.mode == "f" or self.mode == "b":
            self.fhandle.write(string+"\n")
        return
    def close(self):
        if self.mode == "f" or self.mode == "b":
            print("Closing file: ", self.filename)
            self.fhandle.close()
        return

