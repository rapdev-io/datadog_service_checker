The code contained in this directory is intended to poll dashboards, monitors, notesbooks, and SLOS
to determine what may be affected by changes to the names of services.

This code can be run in one of three ways, though all will require a Datadog API and Application key
in your environment. Prior to executing any of these methods, ensure that you have DD_API_KEY and 
DD_APP_KEY set in your environment.

Python3.9+ is required for the global or virtual environment methods.

- Docker
-- docker build . --tag service_checker
-- docker run -e DD_API_KEY=$DD_API_KEY -e DD_APP_KEY=$DD_APP_KEY service_checker

- Python (virtual environment)
  - python3 -m venv venv
  - source venv/bin/activate
  - pip install -r requirements.txt
  - python3 service_checker.py

- Python (global install [not recommended])
  - pip install -r requirements.txt
  - python3 service_checker.py
