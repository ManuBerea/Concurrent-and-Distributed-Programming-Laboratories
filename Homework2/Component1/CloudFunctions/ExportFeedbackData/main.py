import functions_framework
from google.cloud import firestore, storage, dataproc_v1
import csv
from io import StringIO
from google.cloud.dataproc_v1 import JobControllerClient
from google.cloud.dataproc_v1.types import Job, JobPlacement, PySparkJob


# Firestore and Storage clients
db = firestore.Client()
storage_client = storage.Client()
bucket_name = 'feedback_data_bucket'
bucket = storage_client.bucket(bucket_name)

# DataProc job client
region = 'us-central1'
dataproc_client = JobControllerClient(client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

@functions_framework.http
def export_feedback(request):
    feedback_collection = db.collection('restaurants').stream()

    csv_output = StringIO()
    writer = csv.writer(csv_output)
    writer.writerow(['Restaurant Id','Restaurant Name', 'Restaurant Location', 'User ID', 'Ambiance', 'Comment', "Food Quality", "Service"])

    for restaurant in feedback_collection:
      restaurant_id = restaurant.id
      restaurant_data = restaurant.to_dict()
      feedback_ref = db.collection('restaurants').document(restaurant_id).collection('feedback')
      feedbacks = feedback_ref.stream()

      for feedback in feedbacks:
          feedback_data = feedback.to_dict()

          writer.writerow([
           restaurant_id,
           restaurant_data.get('name'),
           restaurant_data.get('location'),
           feedback.id,
           feedback_data.get('ambiance'),
           feedback_data.get('comment'),
           feedback_data.get('food_quality'),
           feedback_data.get('service')])

    # Upload CSV to bucket
    csv_output.seek(0)
    blob = bucket.blob('feedback_export.csv');
    blob.upload_from_string(csv_output.getvalue(), content_type='text/csv')

    csv_output.truncate(0)
    csv_output.close()

    # Submit a job to Dataproc to perform analysis
    script_path = 'gs://feedback_data_bucket/feedback_data_analysis.py'
    cluster_name = 'feedback-analysis-cluster'
    project_id = 'feedbackanalysis-417819'

    job_result = submit_dataproc_job(script_path, cluster_name, region, project_id)

    return 'Feedback data exported and job submitted successfully', 200


def submit_dataproc_job(script_path, cluster_name, region, project_id):
    job = {
        'placement': JobPlacement(cluster_name=cluster_name),
        'pyspark_job': PySparkJob(main_python_file_uri=script_path),
    }

    result = dataproc_client.submit_job(project_id=project_id, region=region, job=Job(**job))
    return result