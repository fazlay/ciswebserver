from flask import Flask
from scraper import scraper 
import pandas as pd
app = Flask(__name__)

@app.route('/')
def start_scraping():
    try:
        scraper()  # calls the original scraper function
        data = pd.read_csv("cis.csv").values.tolist() 
        return data
    except Exception as e:
        return f'Error occurred: {str(e)}', 500

if __name__ == '__main__':
    app.run(debug=True)

@app.route('/about')
def about():
    return 'About'