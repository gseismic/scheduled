from flask import Flask, request, jsonify, render_template


app = Flask(__name__)


@app.route('/')
def index():
    render_template('index')
