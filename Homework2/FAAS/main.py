import functions_framework
from google.cloud import firestore

database = firestore.Client()


@functions_framework.http
def add_feedback(request):
    request_json = request.get_json()

    restaurant_id = request_json['restaurant_id']
    payload = request_json['payload']

    restaurant_ref = database.collection('restaurants').document(restaurant_id)
    restaurant_data = restaurant_ref.get()
    if not restaurant_data.exists:
        return 'Restaurant not found', 404

    feedback_data = restaurant_data.collection('feedback')
    if validate_vote(feedback_data, payload):
        feedback_data.add(payload)
    else:
        return 'You have already voted for this restaurant!', 400

    return 'Vote added', 200


def validate_vote(feedback, payload):
    for vote in feedback.stream():
        vote_data = vote.to_dict()

        if vote_data['feedback_id'] == payload['user_id']:
            return False

    return True
