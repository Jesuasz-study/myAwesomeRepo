from asyncore import write
from cgitb import text
import time
import os
import codecs #for encode with utf-8
import csv
from fileinput import filename
import requests #for send HTTP requests 
from bs4 import BeautifulSoup #For pulling data out of HTML files
from urllib.parse import urljoin #Construct a absolute URL combining two url's 
from concurrent.futures import ThreadPoolExecutor #Asynchronus threads 
import asyncio # Concurrent tasks, through coroutines


#Get the links from the table and return a list with these links
def get_links():
    main_page = 'https://en.wikipedia.org/wiki/Member_states_of_NATO'
    response = requests.get(main_page)
    soup = BeautifulSoup(response.text, "lxml")
    selector = 'p+table td:nth-child(2) > a, p+table td:nth-child(1) > a:nth-child(1)' #takes links from the second column from a table
    all_links = soup.select(selector)
    links = []
    for single_link in all_links: #append the links into a list
        link = single_link.get("href")
        link = urljoin(main_page,link)
        links.append(link)
    return links


#Create & Save the .html docs from countries, elment by element from a list
def fetch(link):
    response = requests.get(link) 
    filename = os.path.dirname(__file__)+"/scraping/"+link.split("/")[-1]+".html" #Path for create & save the .html file
    with open (filename, 'wb') as file:
        file.write(response.content)


#Take .html's one by one and calls the function for scrap the content
async def open_htmls(links):
    count = 0 #used for write the header for the .csv just once
    tasks = []
    for single_link in links: 
        task = asyncio.ensure_future(single_html(single_link,count))
        tasks.append(task)
        count += 1


#Scrap the data from a .html taking the info inside a table (take pairs (td - th) and the info inside)
async def single_html(link_name,count):
    formated_name = link_name.split("/")[-1]
    filename = os.path.dirname(__file__)+"/scraping/"+formated_name+".html"
    if filename == "c:/Users/poroj/Documents/Python Scripts/Concurrency example/scraping/Military_of_Iceland.html":
        return # Military_of_Iceland.html is excluded because it doesn't content the table we are interested for
    with codecs.open(filename, 'rb', 'utf-8') as file:
        contents = file.read()
        soup = BeautifulSoup(contents, 'html.parser')
        gdp_table_label = soup.find_all("th", class_="infobox-label") 
        gdp_table_data = soup.find_all("td", class_="infobox-data")
        table_data = []
        table_data.append(formated_name)
        table_label = []
        table_label.append("Country")
        for td_data in gdp_table_data:
            table_data.append(td_data.text.replace('\n', ' ').strip()) # forrmating text, replacing the "\" character with a blank space
        for td_label in gdp_table_label:
            table_label.append(td_label.text.replace('\n', ' ').strip())
    save_csv(table_label,table_data,count)


#Save the data scraped and writes on a .csv 
def save_csv(label_list,data_list,count):
    dic_result = dict(zip(label_list, data_list)) #zip the lists, crating a dictionary with the data
    list_results = [dic_result]
    filename = os.path.dirname(__file__)+"/csv/"+"Output_Data_OTAN"+".csv"
    with codecs.open(filename,'ab','utf-8') as file: #codecs is for formating with utfs
        writer = csv.DictWriter(file, fieldnames=label_list)
        if count == 0: #write the header just once 
            writer.writeheader()
        writer.writerows(list_results)


if __name__ == '__main__':
    start_time = time.time()
#-----
    links = get_links()
#-----
    with ThreadPoolExecutor(max_workers=8) as pool:
        pool.map(fetch,links)
#-----
    asyncio.get_event_loop().run_until_complete(open_htmls(links))
#-----
    print('\nTotal Time:', time.time() - start_time)