import requests
import json

# 更改ip
url = 'http://140.114.77.150:8000/items'
input_json = {
    'id': 0,
    'title': "This is input",
    'content': [
        {
            "sentence":{
                "origin": "Apple is looking at buying U.K. startup for $1 billion",
            }
        }
    ],
    "pipline":{
        'Tok': 1 ,
        'Pos': 1,
        'Tag': 1,
        'Ner': 1,
        'Dep': 1
        },
 
    "metadata":{}
}

res = requests.post(url, data = json.dumps(input_json))
print(res.text)