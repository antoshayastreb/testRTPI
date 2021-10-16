import configparser

config = configparser.ConfigParser()
#Токен
try:
  config.read("settings.ini")
  token = config["Base"]["token_string"]
except configparser.ParsingError as p:
  print(p)
  token = ""