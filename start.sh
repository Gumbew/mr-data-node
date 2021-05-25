#!/bin/bash
export $(grep -v "^#" .env | xargs)
uvicorn main:app --port=5002 --reload