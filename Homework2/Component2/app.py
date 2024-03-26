import json
import os
import requests

from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from flask_socketio import SocketIO
from google.cloud import firestore
from google.cloud import pubsub_v1

app = Flask(__name__)
CORS(app)
load_dotenv()

app.config['DEBUG'] = os.environ.get('DEBUG')

CLOUD_FUNCTION_URL_ADD = os.environ.get('CLOUD_FUNCTION_URL_ADD')
CLOUD_FUNCTION_URL_GET_ANALYSIS = os.environ.get('CLOUD_FUNCTION_URL_GET_ANALYSIS')

database = firestore.Client()
socketio = SocketIO(app, cors_allowed_origins="*")
publisher = pubsub_v1.PublisherClient()

project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
topic_id = 'feedback-updates'
subscription_id = 'feedback-updates-sub'
topic_path = publisher.topic_path(project_id, topic_id)


def publish_feedback_update(feedback):
    """Publish feedback to the Pub/Sub topic."""
    message_data = json.dumps(feedback).encode('utf-8')
    future = publisher.publish(topic_path, message_data)
    return future.result()


def callback(message):
    try:
        data_json = json.loads(message.data.decode('utf-8'))
        print(f"Received message: {data_json}")
        socketio.emit('feedback_update', data_json)
    except json.JSONDecodeError as e:
        print(f"Error decoding message data: {e}")
    finally:
        message.ack()


def listen_for_feedback_updates():
    """Background task to listen for new feedback messages in the Pub/Sub subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(os.environ.get('GOOGLE_CLOUD_PROJECT'), 'feedback-updates-sub')

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"Listening for messages on {subscription_path} threw an exception: {e}")


@app.route("/submit", methods=["POST"])
def submit_feedback():
    data = request.json
    restaurant_id = data.get("restaurant_id")
    user_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    feedback = data.get("feedback")

    restaurant_name = get_restaurant_name(restaurant_id)
    if restaurant_name is None:
        return jsonify({"status": "error", "message": "Restaurant not found"}), 404

    payload = {
        "restaurant_id": restaurant_id,
        "restaurant_name": restaurant_name,
        "payload": {
            "user_id": user_ip,
            "feedback": feedback
        }
    }

    response = requests.post(CLOUD_FUNCTION_URL_ADD, json=payload)

    if response.status_code == 200:
        feedback_with_name = feedback
        feedback_with_name['restaurant_name'] = restaurant_name
        publish_feedback_update({"feedback": feedback_with_name})
        return jsonify({"status": "success", "message": "Review submitted successfully!"})
    else:
        return jsonify({"status": "error", "message": "Error occurred"}), response.status_code


@app.route("/allRestaurants", methods=["GET"])
def all_restaurants():
    try:
        restaurants_ref = database.collection('restaurants')
        docs = restaurants_ref.stream()

        restaurants = [{"id": doc.id, "name": doc.to_dict().get("name", "No Name")} for doc in docs]

        return jsonify(restaurants), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/allReviews", methods=["GET"])
def all_reviews():
    try:
        reviews = []
        count = 0
        restaurants_ref = database.collection('restaurants')
        restaurants = restaurants_ref.stream()

        for restaurant in restaurants:
            restaurant_data = restaurant.to_dict()
            restaurant_name = restaurant_data.get('name', 'No Name')
            restaurant_location = restaurant_data.get('location', 'No Location')

            feedback_ref = restaurant.reference.collection('feedback')
            feedbacks = feedback_ref.stream()

            for feedback in feedbacks:
                review = feedback.to_dict()
                review['restaurant_id'] = restaurant.id
                review['restaurant_name'] = restaurant_name
                review['restaurant_location'] = restaurant_location
                review['feedback_id'] = feedback.id
                reviews.append(review)
                count += 1

        reviews.append(count)
        return jsonify(reviews), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/avgRatings", methods=["GET"])
def avg_ratings():
    response = requests.get(CLOUD_FUNCTION_URL_GET_ANALYSIS)

    if response.status_code == 200:
        return jsonify(response.json()), 200
    else:
        return jsonify({"status": "error", "message": "Error occurred"}), response.status_code


def get_restaurant_name(restaurant_id):
    try:
        restaurant_ref = database.collection('restaurants').document(restaurant_id)
        restaurant = restaurant_ref.get()
        if restaurant.exists:
            return restaurant.to_dict().get('name')
        else:
            return None
    except Exception as e:
        print(f"An error occurred while fetching restaurant data: {e}")
        return None


@app.route("/")
def home():
    return render_template("index.html")


socketio.start_background_task(listen_for_feedback_updates)

if __name__ == "__main__":
    socketio.run(app, debug=app.config['DEBUG'])
