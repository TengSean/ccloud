
import os ,json

from typing import List
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()

class pipline_dict(BaseModel):
    Tok: int 
    Pos: int
    Tag: int
    Ner: int
    Dep: int

    
class sentence_dict(BaseModel):
    origin: str

class content_list(BaseModel):
    sentence: sentence_dict
        
class input_json(BaseModel):
  id: int
  title: str = "This is input"
  content: List[content_list]
  pipline: pipline_dict
  metadata: dict={}


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/items/")
async def run(item: input_json):
    paragraph = item.content[0].sentence.origin
    pipline = item.pipline.dict()
    
    os.system("PYTHONPATH='.' luigi --module start Entry --input-path testing.json --input-pipline {} --input-paragraph {} --local-scheduler".format('\''+json.dumps(pipline)+'\'', '\''+paragraph+'\''))
    with open('./data/output/testing.json', 'r') as f:
        return_json = json.load(f)
        
    return return_json
