# CCloud final
cmd: PYTHONPATH='.' luigi --module start Entry --input-path 20200620.json --local-scheduler



## fastapi download:
    pip install fastapi
    pip install uvicorn


## Run fastapi server:
    cmd: uvicorn --host 0.0.0.0 {your_script_name}:app
