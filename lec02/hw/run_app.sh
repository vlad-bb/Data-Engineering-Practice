#!/bin/bash

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
else
    echo "Virtual environment already exists"
fi

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Installing requirements..."
pip install -r requirements.txt
pip install --upgrade pip

echo "Running tests..."
source .venv/bin/activate && pytest job1/tests job2/tests -v

echo "Running all jobs..."
python entry_point.py

