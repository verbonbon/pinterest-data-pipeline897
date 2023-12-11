import requests
import json
import datetime

pin_data = {'index': 5,
            'unique_id': '08fabbb2-8b80-41e8-871c-759837ed816f',
            'title': 'Bulgarian Artist Makes Incredible Illustrations That Glow From Within',
            'description': "It doesn't matter if you use a pencil, a crayon or the tip\
             of your nose to create art, it is no small feat to produce something that'll\
             knock everyone's socks off. Some artists…",
            'poster_name': 'Bored Panda', 
            'follower_count': '2M', 'tag_list': 'Girl Drawing Sketches, Pencil Art Drawings,\
             Cool Art Drawings,Beautiful Drawings,Easy Drawings,Drawing Tips, Beautiful Artwork,\
             Drawing Drawing,Amazing Artwork',
            'is_image_or_video': 'image',
            'image_src': 'https://i.pinimg.com/originals/5f/38/b0/5f38b0752da44335c943df76559cefbf.jpg', 'downloaded': 1, 'save_location': 'Local save in /data/art', 'category': 'art'}

geo_data = {'ind': 5,
            'timestamp': datetime.datetime(2018, 8, 29, 13, 27, 39),
            'latitude': -88.8298,
            'longitude': -170.188,
            'country': 'Albania'}

user_data = {'ind': 5, 'first_name': 'Adam', 'last_name': 'Acosta',
             'age': 20, 'date_joined':
             datetime.datetime(2015, 10, 21, 21, 26, 45)}

invoke_url=f"https://bj0k1iog5m.execute-api.us-east-1.amazonaws.com/production/topics/0a5e6ec37a2f.pin"
# Send JSON messages to follow this structure
payload = json.dumps({
    "records": [
        {
        #Data should be send as pairs of column_name:value, with different columns separated by commas       
        "value": {"index": pin_data["index"], 
                  "unique_id": pin_data["unique_id"],
                  "title": pin_data["title"], 
                  "description": pin_data["description"],
                  "poster_name": pin_data["poster_name"],
                  "follower_count": pin_data["follower_count"],
                  "is_image_or_video": pin_data["is_image_or_video"],
                  "image_src": pin_data["image_src"]
                  }
        }
    ]
})


headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
response = requests.request("POST", invoke_url, headers=headers, data=payload)

print(response.status_code)
