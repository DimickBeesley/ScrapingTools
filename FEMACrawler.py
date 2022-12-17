import pandas as pd
import re

from requests_html import HTMLSession
from bs4 import BeautifulSoup


class Crawler:

    data = pd.DataFrame(columns=['disasterNumber', 'amendmentNumber', 'effectiveDate', 'categoryCode', 'assistanceType', 'GEOID'])
    
    base_url = "http://www.placeholder.com"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}

    # Useful regular expressions for parsing the documents
    regex = {
        'category_code' : r'(\[|\()Categories\s[A-G]((\s(and|-)\s)|-)[A-G](\]|\))|Category\s*[A-G]',
        'category_code_letters' : r'(?<=[\s-])\b[A-Ga-g]\b',
        'county_groups': r'(([A-Z][A-Za-z.-]*(\s[A-Z][a-z]*)?)+(,\s?|\s?and\s?|,\s?and\s?|\s?)+)+',
        'individual_county' : r'([A-Z][A-Za-z.-]*(\s[A-Z][a-z]*)?)+(,\s?|\s?and\s?|,\s?and\s?|\s?)+',
        'assistance_type' : r'([Ii]ndividual|[Pp]ublic)\s[Aa]ssistance',
        'effective_date' : r'(?<=EFFECTIVE DATE:)[\s\?]*(([A-Z][a-z]*)[\s\,]*(\d{1,2})[\s\,]*(\d{4}))', # TODO: Check that change to DATE from EFFECTIVE DATE okey
        'effective_date_var_2' : r'(?<=DATE:).*[\s\?]*(([A-Z][a-z]*)\s*?(\d{1,2})[\s\,]*?(\d{4}))',
        'state': r'(?<=\[[A-Z]{4}-\d{4}-[A-Z]{2}\])\s*.{1,50}[;:]',
        'state_var_2': r'(?<=[A-Z]{4}-\d{4}-\d{4})\]?\s*.{1,50}[;:]',
        'state_var_3': r'(?<=[A-Z]{4}-\d{4}-\d{3})\]?\s*.{1,50}[;:]',
        'state_var_4': r'(?<=[A-Z]{4}-\d{4}-\d{4})\]?\s*.{1,50}[Aa]mendment',
        'amendment_number': r'(?<=amendment)(-no)?-?(\d*)'
    }





    """ TRAVERSAL
    """

    # Starts the tree structured traversal of each disaster whose number is contained
    # in the list given to this function
    def start_scrape(self, disaster_numbers):
        
        session = HTMLSession()

        for number in disaster_numbers:

            r = session.get("http://www.fema.gov/disaster/{}/notices".format(1603), headers=self.headers)
            first_page_soup = BeautifulSoup(r.text, 'html.parser')

            self.crawl_notice_pages(session, first_page_soup, number)
        
        return
    
    # Takes a soup object of the first page of notices. Looks for aditional pages.
    # For each page including this one. Call crawl notices and pass along the
    # disaster number
    def crawl_notice_pages(self, session, first_page_soup, disaster_number):

        page_urls = self.find_next_page_urls(first_page_soup, disaster_number)

        for url in page_urls:
            r = session.get(url, headers=self.headers)
            second_page_soup = BeautifulSoup(r.text, 'html.parser')
            self.crawl_notices(session, second_page_soup, disaster_number)

        return

    def crawl_notices(self, session, second_page_soup, disaster_number):
        
        doc_urls = self.find_notice_urls(second_page_soup)

        for url in doc_urls:
            amendment_number = self.get_amendment_number(url)

            r = session.get(url, headers=self.headers)
            notice_soup = BeautifulSoup(r.text, 'html.parser')

            self.parse_doc(notice_soup, disaster_number, amendment_number)

        return
    



    # Takes in a URL to a notice/ammendment document.
    # Returns amendment number in the url.
    def get_amendment_number(self, notice_url):
        
        out = 0

        if ("amendment" not in notice_url) or (('public' in notice_url) and ('notice' in notice_url)):
            return out
        else:
            out = int(re.search(self.regex['amendment_number'], notice_url).group().strip('-no'))

        return out

    # Takes in notice page html, returns list of urls to other
    # pages associated with this disaster declaration.
    def find_next_page_urls(self, soup, disaster_number):
        
        out = []
        
        # use css selectors to find links within nav tags of the pager class
        links = soup.select('nav.pager a')

        # append urls associated to current disaster declaration, that are not this url
        for link in links:
            out.append("http://www.fema.gov/disaster/{}/notices".format(disaster_number) + link.attrs['href'])
        
        return out

    # Takes in notice page html, returns list of urls to notices on
    # this webpage
    def find_notice_urls(self, soup):
        
        out = []

        # use css selectors to find links within h3 tags of the field-content class
        links = soup.select('h3.field-content a')
        
        # append only urls to notices to output list
        for link in links:
            if ('/disaster-federal-register-notice/' in link.attrs['href']) and ('initial-notice' in link.attrs['href']):
                out.append(self.base_url + link.attrs['href'])
            if ('/disaster-federal-register-notice/' in link.attrs['href']) and ('amendment' in link.attrs['href']):
                out.append(self.base_url + link.attrs['href'])

        return out

  



if __name__ == "__main__":
    
    # Instantiate Crawler Object
    crawler = Crawler()

    crawler.do_the_thing()
    

    