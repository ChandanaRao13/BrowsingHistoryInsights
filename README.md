Mongo Import and Export commands:
Mongo Export:
$ mongoexport -d browsinghistory -c chromedata -o chromedata.json
$ mongoexport -d browsinghistory -c URLRepository -o URLRepository.json


Mongo Import:
$ mongoimport --db browsinghistory --collection URLRepository --file /PersonalFiles/MSSE/Semester3/CMPE239/Project/JavaProject/dataminingengine/URL*.json
