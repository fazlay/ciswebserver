from functools import reduce
from lxml import html
import requests
import operator
import json
import csv
import os
import re 
from concurrent.futures import ThreadPoolExecutor


# https://cis.scc.virginia.gov/EntitySearch/Index
OUTPUT_FILE_NAME = "cis.csv"
OUTPUT_FILE_NAME2 = "cis_without_email.csv"

COOKIES = {
    'ASP.NET_SessionId': '1wbmr4xfkxjct1z3vtz3qaa5',

}

def read_json(file_name):

    with open(file_name, 'r',encoding='utf-8-sig') as file:
        my_list = json.load(file)
    return my_list

def getFromDict(dataDict, mapList):
    return reduce(operator.getitem, mapList, dataDict)


def json_to_text(json_data, keys):

    try:
        return getFromDict(json_data, keys)

    except:
        return ""


def xpath_to_text(webpage, xpath):
    """Conver elemnt to text"""

    try:
        return webpage.xpath(xpath)[0].strip()
    except:
        return ""

def save_csv(filename, data_list, isFirst=False, removeAtStarting=True):
    """Save data to csv file"""

    if isFirst:
        if os.path.isfile(filename):
            if removeAtStarting:
                os.remove(filename)
            else:
                pass

    with open(f'{filename}', "a", newline='', encoding='utf-8-sig') as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(data_list)


def send_requests(url, headers="", cookies="", params="", request_type="GET", response_type="html", data={}):
    """Request sending Function"""
    proxies = {
        'http': 'http://scraperapi.country_code=us:43d61359f1031da410e86a26631ae299@proxy-server.scraperapi.com:8001',
        'https': 'http://scraperapi.country_code=us:43d61359f1031da410e86a26631ae299@proxy-server.scraperapi.com:8001',
    }
    if request_type == "POST":
        response = requests.post(url, headers=headers,proxies=proxies,verify=False,
                                 cookies=cookies, params=params,data=data)

    else:
        response = requests.get(url, headers=headers,proxies=proxies,verify=False,
                                cookies=cookies, params=params)

    if response_type == "json":
        return response.json(), response
    else:
        webpage = html.fromstring(response.content)
        return webpage, response


# 11762015
def result_generator(url,filter_data):
    formation_start_date = json_to_text(filter_data,["formation_start_date"])
    formation_end_date = json_to_text(filter_data,["formation_end_date"])
    filter_fields = f'businessName=|businessID=|principalName=|principalLastName=|principalIndividualNameSearch=0|principalEntityNameSearch=0|agentName=|agentLastname=|agentIndividualNameSearch=0|agentEntityNameSearch=0|filingNumber=|businessType=|jurisdictionType=|businessStatus=|nameType=|documentType=|industryCode=|formationStart={formation_start_date}|formationEnd={formation_end_date}|addressType=0|streetAddress1=|city=|zipCode=|RegisterOfficeAddressCheck=0|IsSeriesLLC='
    print(filter_fields)
    data = {
        'SearchType': 'othersearchtype',
        'IsOnline': 'true',
        'IsDownloadReports': '',
        'QuickSearch.BESearchLogic': '2',
        'QuickSearch.StartsWith': '2',
        'QuickSearch.Contains': '0',
        'QuickSearch.ExactMatch': '0',
        'QuickSearch.BusinessName': '',
        'QuickSearch.BusinessId': '',
        'QuickSearch.PrincipalName': '',
        'QuickSearch.principalLastName': '',
        'QuickSearch.AgentName': '',
        'QuickSearch.AgentLastName': '',
        'QuickSearch.Addresstype': '0',
        'QuickSearch.PrincipalFirstName': '',
        'QuickSearch.PrincipalLastName': '',
        'QuickSearch.AgentFirstName': '',
        'QuickSearch.FilingNumber': '',
        'QuickSearch.AgentNameIndividualSearch': '1',
        'QuickSearch.AgentNameEntitySearch': '0',
        'QuickSearch.PrincipalNameIndividualSearch': '1',
        'QuickSearch.PrincipalNameEntityeSearch': '0',
        'AdvancedSearch.BusinessTypeID': '',
        'AdvancedSearch.BusinessStatusID': '',
        'AdvancedSearch.NameType': '',
        'AdvancedSearch.DocumentTypeID': '',
        'AdvancedSearch.IndustryCodeID': '',
        'AdvancedSearch.FilingDateFrom': '',
        'AdvancedSearch.FilingDateTo': '',
        'AdvancedSearch.PrincipalOfficeAddressSearch': '0',
        'AdvancedSearch.AgentOfficeAddressSearch': '0',
        'AdvancedSearch.StreetAddress1': '',
        'AdvancedSearch.City': '',
        'AdvancedSearch.ZipCode': '',
        'AdvancedSearch.SeriesLLC': '',
        'AdvancedSearch.Fields': filter_fields,
        # '__RequestVerificationToken': 'Cxus3Iug7SYPmb48aDcUi8Oemdlm5OJOfRKZJaclbExMwCweB9qKndHmbelGUH2SNd1a3ozAGV-PKIWE4F8UU4hLyt_dhQJM_vheyi_QIuU1',
    }

    webpage,response = send_requests(url,request_type='POST',data=data,cookies=COOKIES)


def extract_zipcode(address):
    try:
        zipcode = address.split(",")[-2].strip().split("-")[0].strip()
        return zipcode
    except: 
        return ""

def scrape_each_entity(entity_id):
    url = f"https://cis.scc.virginia.gov/EntitySearch/BusinessInformation?businessId={entity_id}&source=FromEntityResult&isSeries%20=%20false"
    webpage,response = send_requests(url)
    name = xpath_to_text(webpage,"//div[text() = ' Name:']/following::div[1]/text()")
    if name == "": 
        name = xpath_to_text(webpage,"//table[@id = 'grid_principalList']/tbody/tr[1]/td[1]/text()")
    entity_name = xpath_to_text(webpage,"//div[text() = 'Entity Name:']/following::div[1]/text()")
    entity_type = xpath_to_text(webpage,"//div[text() = 'Entity Type:']/following::div[1]/text()")
    formation_date = xpath_to_text(webpage,"//div[text() = 'Formation Date:']/following::div[1]/text()")
    email = xpath_to_text(webpage,"//div[text() = 'Email Address:']/following::div[1]/text()")
    address = xpath_to_text(webpage,"//div[text()=' Address:']/following::div[1]/text()")
    zipcode = extract_zipcode(address)
    
    data_list = [url,entity_id,entity_name,entity_type,formation_date,address,zipcode,name,email]
    print(data_list)
    return data_list



def get_totals(text):
    for attempt in range(2):  # Allow one retry
        try:
            match = re.search(r'Page \d+ of (\d+), records \d+ to \d+ of (\d+)', text)
            if match:
                total_pages = int(match.group(1))
                total_entities = int(match.group(2))
                return total_pages, total_entities
            raise ValueError("Pattern not found")
        except (ValueError, AttributeError) as e:
            if attempt < 1:  # If this is the first attempt, retry
                print(f"Error: {e}. Retrying...")
            else:
                print("Failed to extract totals after retry.")
                return "", ""  # Return empty strings on failure


def get_page_info(url): 
    data = {'undefined': '', 'sortby': '', 'stype': 'a', 'pidx': '1'}
    webpage,response = send_requests(url,request_type='POST',data=data,cookies=COOKIES)
    page_info = xpath_to_text(webpage,"//li[@class = 'pageinfo']/text()")
    total_pages,total_entities = get_totals(page_info)
    print(f"There are total {total_entities} in {total_pages} pages!")
    return total_pages,total_entities,webpage


def scrape_data(url,filter_data): 
    result_generator(url,filter_data)
    total_page,total_entity_found,webpage_data = get_page_info(url)

    for page in range(1,2):
        data = {'undefined': '', 'sortby': '', 'stype': 'a', 'pidx': f'{page}'}
        if page == 1: 
            webpage = webpage_data
        else:
            webpage,response = send_requests(url,request_type='POST',data=data,cookies=COOKIES)
        all_entity = webpage.xpath("//table[@id = 'grid_businessList']/tbody/tr/td/a/text()")
        print(f"Total entity in Page {page} is : {len(all_entity)}")
        for idx,each_entinty in enumerate(all_entity[:4]):
            if idx==4: 
               break
            data_list = scrape_each_entity(each_entinty)
            if data_list[-1] == "N/A":
                save_csv(OUTPUT_FILE_NAME2,data_list)
            else:
                save_csv(OUTPUT_FILE_NAME,data_list)
            print(f"Completed {(idx+1)+(page-1)*25}/{total_entity_found} || PAGE: {page}/{total_page}")

        # with ThreadPoolExecutor(max_workers=500) as e:
        #     list_data_list = list(e.map(scrape_each_entity,all_entity))
        
        # for data_list in list_data_list: 
        #     if data_list[-1] == "N/A":
        #         save_csv(OUTPUT_FILE_NAME2,data_list)
        #     else:
        #         save_csv(OUTPUT_FILE_NAME,data_list)
        # print(f"Completed: {page*len(all_entity)}/{total_entity_found} || PAGE: {page}/{total_page}")



def scraper():
    global OUTPUT_FILE_NAME
    # OUTPUT_FILE_NAME = "output.csv"
    url = "https://cis.scc.virginia.gov/EntitySearch/Index"
    header_list = []
    filter_dict = {
        "formation_start_date": "10/25/2024",
        "formation_end_date" : "10/26/2024"
    }
    scrape_data(url,filter_dict)


def main():
    scraper()


if __name__ == "__main__":
    main()
