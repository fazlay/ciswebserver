from flask import Flask
from scraper import scraper 
app = Flask(__name__)

@app.route('/')
def start_scraping():
    try:
        scraper()  # calls the original scraper function
        return 'Scraping completed! Check the CSV files for results.'
    except Exception as e:
        return f'Error occurred: {str(e)}', 500

if __name__ == '__main__':
    app.run(debug=True)

@app.route('/about')
def about():
    return 'About'