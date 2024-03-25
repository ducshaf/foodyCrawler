import json

with open('data.json', encoding='utf-8') as f:
    data = json.load(f)
    res = json.loads(data['body'])
    searchItems = res['searchItems']
    print(searchItems[0]['Address'])
    print(searchItems[0]['DetailUrl'])
    print(searchItems[0]['Latitude'])
    print(searchItems[0]['Longitude'])
    print(searchItems[0]['MobilePicturePath'])
    print(searchItems[0]['PicturePathLarge'])
