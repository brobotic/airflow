#!/bin/bash

/home/micha/github/airflow/venv/bin/ansible-playbook -i ansible/inventory.ini ansible/deploy_airflow.yml
