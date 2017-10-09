import json

def trim_path(path):
    if path[-1] =='/':
        return path[:-1]
    else:
        return path

def json_pretty(jsonMap):
    return json.dumps(jsonMap,indent=4, sort_keys=True)