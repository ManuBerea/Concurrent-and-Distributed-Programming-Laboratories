# Create Python virtual environment:
```bash
python -m venv env
```

# Start virtual environment:

```bash
./env/Scripts/activate
```

* If the error: "cannot be loaded because running scripts is disabled on this system. For more information, see about_Execution_Policies." occurs open a PowerShell with administrator and run the command:

# Create python virtual environment:

```bash
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted
```

# Install dependencies in the virtual environment:

```bash
pip install flask
pip install python-dotenv
pip install gunicorn
pip install flask-cors
pip install flask_socketio
```

## Google cloud dependencies

```bash
pip install google-cloud-firestore
pip install google-cloud-pubsub
```

## Install the gcloud CLI: [Google Cloud SDK Installation Guide](https://cloud.google.com/sdk/docs/install)

## After the install is complete:

```bash
gcloud init #( you can check the box after install or run the command in a terminal ( make sure to open the terminal after the install to recognize the command))
```

## Prepare for deployment:

```bash
pip freeze > requirements.txt #( pip freeze - Output installed packages in requirements format.)
```

## Deploy:

```bash
gcloud run deploy --source .
```