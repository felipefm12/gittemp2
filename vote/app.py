from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging
import math
import pandas as pd

app = Flask(__name__)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

ratings = pd.read_csv('ratings.csv')

def cosine_similarity_manual(vec1, vec2):
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude_vec1 = math.sqrt(sum(a**2 for a in vec1))
    magnitude_vec2 = math.sqrt(sum(b**2 for b in vec2))
    
    if magnitude_vec1 == 0 or magnitude_vec2 == 0:
        return 0  # Evitar la división por cero
    
    return dot_product / (magnitude_vec1 * magnitude_vec2)

def find_nearest_neighbors_manual(user_id, ratings_df, num_neighbors=10):
    user_ratings = ratings_df[ratings_df['userId'] == user_id].drop(columns=['userId', 'timestamp']).values[0]
    other_users_ratings = ratings_df[ratings_df['userId'] != user_id].drop(columns=['userId', 'timestamp'])

    similarities = [
        cosine_similarity_manual(user_ratings, row.values)
        for _, row in other_users_ratings.iterrows()
    ]

    nearest_neighbors_indices = sorted(range(len(similarities)), key=lambda i: similarities[i], reverse=True)[:num_neighbors]
    nearest_neighbors_ids = ratings_df.iloc[nearest_neighbors_indices]['userId'].tolist()

    return nearest_neighbors_ids

@app.route("/", methods=['POST', 'GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    if request.method == 'POST':
        redis = get_redis()
        
        if 'calculate' in request.form:
            user_id = int(request.form.get('user_id'))
            
            cosine_neighbors = find_nearest_neighbors_manual(user_id, ratings)
            
            # Convertir la lista a formato JSON antes de almacenar en Redis
            neighbors_data = json.dumps({'user_id': user_id, 'neighbors': cosine_neighbors})
            redis.rpush('cosine_neighbors', neighbors_data)
            app.logger.info(neighbors_data)
            if redis.exists('cosine_neighbors'):
                app.logger.info('Data uploaded to Redis successfully')
            else:
                app.logger.error('Failed to upload data to Redis')

    resp = make_response(render_template(
        'index.html',
        option_a=os.getenv('OPTION_A', "Cats"),
        option_b=os.getenv('OPTION_B', "Dogs"),
        hostname=socket.gethostname(),
        similarity=None,
        ratings_data=None,
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)