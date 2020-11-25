import os
from flask import Flask, request, jsonify, render_template


template_folder = os.path.join(os.path.dirname(__file__), 'templates')
app = Flask(__name__, template_folder=template_folder)


@app.route('/')
def index():
    return render_template('index.html')
