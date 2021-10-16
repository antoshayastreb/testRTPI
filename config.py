import configparser

config = configparser.ConfigParser()
#Токен
try:
  config.read("settings.ini")
  token = config["Base"]["token_string"]
  host = config["DBconnection"]["host"]
  database = config["DBconnection"]["database"]
  user = config["DBconnection"]["user"]
  password = config["DBconnection"]["password"]
except configparser.ParsingError as p:
  print(p)
  token = ""
  host = "localhost"
  database = ""
  user = ""
  password = ""


base_url='http://rtpiapi.hrdn.io/'