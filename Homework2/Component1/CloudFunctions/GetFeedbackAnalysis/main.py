import functions_framework
from google.cloud import firestore, storage
import csv
from flask import jsonify
import json

# Storage client
storage_client = storage.Client()
bucket_name = 'feedback_data_bucket'
bucket = storage_client.bucket(bucket_name)

@functions_framework.http
def get_feedback_analysis(request):
    analysis_folder = 'restaurant_feedback_analysis.json/'
    blobs = bucket.list_blobs(prefix=analysis_folder)
    analysis_data = []

    analysis_blob = None
    for blob in blobs:
        if "part-00000" in blob.name and blob.name.endswith(".json"):
            analysis_blob = blob
            break

    if analysis_blob:
        # Download the blob's content
        lines = analysis_blob.download_as_string().decode('utf-8').strip().split('\n')

        for line in lines:
                analysis_data.append(json.loads(line))

        return jsonify(analysis_data), 200
    else:
        return 'No analysis results found', 404
