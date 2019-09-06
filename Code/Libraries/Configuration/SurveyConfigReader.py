
import configparser

class surveyConfig():
    def __init__(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read('app.ini')
            #self.config.read_file(open(r'C:\Users\WilliamAndrus\Documents\GitHub\PugetSoundSurveyETL\Code\Libraries\Configuration\app.ini'))
            #if len(self.config) != len('app.ini'):
            #    raise
        except:
            print('Couldn not find app.ini file')
    
    def get(self,section,key):
        return self.config.get(section,key)

    def getboolean(self, section,key):
        return self.config.getboolean(section, key)

"""
Used for testing logic
"""
if __name__ == '__main__':
    cfg = surveyConfig()
    print(cfg.get('SQLServer','SERVER'))

    