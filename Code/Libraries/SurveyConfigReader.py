
import configparser

class surveyConfig():
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('app.ini')
    
    def get(self,section,key):
        return self.config.get(section,key)

"""
Used for testing logic
"""
if __name__ == '__main__':
    cfg = surveyConfig()
    print(cfg.get('SQLServer','SERVER'))

    