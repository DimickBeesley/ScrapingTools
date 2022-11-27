import pandas as pd
import re

from requests_html import HTMLSession
from bs4 import BeautifulSoup
from simpledbf import Dbf5


class FEMACrawler:

    data = pd.DataFrame(columns=['disasterNumber', 'amendmentNumber', 'effectiveDate', 'categoryCode', 'assistanceType', 'GEOID'])
    #missing_disaster_numbers = np.array
    url_number = 0

    base_url = "http://www.fema.gov"
    notice_url = base_url + "/disaster/{}/notices".format(url_number)

    state_file_path = r"C:\Users\Dimick\Projects\WebScraping\openFEMA_Scrape\data\tl_2021_us_state\tl_2021_us_state.dbf"
    county_file_path = r"C:\Users\Dimick\Projects\WebScraping\openFEMA_Scrape\data\tl_2021_us_county\tl_2021_us_county.dbf"

    state_geo_data = Dbf5(state_file_path).to_dataframe()
    county_geo_data = Dbf5(county_file_path).to_dataframe()

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

  











    """
    """

    # takes in a string and a regular expression returns a dictionary with both a list of strings from matches
    # (key: 'extracted'), the original string with the matches removed (key: 'text'), and the match objects 
    # (key: 'matches') for later use.
    def extract_from_string(self, pattern, text):
        pattern = re.compile(pattern)
        matches = pattern.finditer(text)
        
        out = {'text': text, 'extracted': [], 'matches': []}

        # Put all match objects from iterable into list so we can use span attribute for slicing and
        # and also iterate through the matches in reverse order easily.
        for match in matches:
            out['matches'].append(match)
        # Reverse the new matches list as well as save list of match objects in the same order for returning
        out['matches'].reverse()

        # Use python string slicing to take the last match in text and put into our 'extracted' list
        for match in out['matches']:
            left = match.span()[0]
            right = match.span()[1]
            out['extracted'].append(out['text'][left:right])
            out['text'] = out['text'][:left] + out['text'][right:]
        out['extracted'].reverse()

        if len(out['text']) == 0:
            out['text']=''

        return out

    # Takes in the soup object of a disaster notice the associated disaster_number and the ammendment number.
    # Adds a new row to the existing pandas dataframe that will hold all of the data we've scraped.
    def parse_doc(self, soup, disaster_number, amendment_number):
        
        elements = soup.select('div.views-row span')
        doc_text = ''
        for element in elements:
            doc_text += element.text
        if doc_text == '':
            return


        new_row = {
            'disasterNumber': disaster_number,
            'amendmentNumber': amendment_number,
            'effectiveDate': None,
            'categoryCode': None,
            'assistanceType': None,
            'GEOID': 00000
        }

        temp_df = pd.DataFrame(columns=['disasterNumber', 'amendmentNumber', 'effectiveDate', 'categoryCode', 'assistanceType', 'GEOID'])

    
        # TODO: Figure out how to deal with these major errors.
        if disaster_number == 1539: # (Amendment 7 missing '[FEMA-1234-DR]')
            return
        if disaster_number == 1607: # (Amendment 6 missing '[' in the [FEMA-1234-DR])
            return
        
        # TODO: learn what to do with Native American reservations
        if disaster_number == 4103: # (Amendment 1 'Eastern Band of Cherokee Indians' where state would be)
            return
        if disaster_number == 4123: # (Amendment 0 'Standing Rock Sioux Tribe' where state would be)
            return
        if disaster_number == 4142: # (Amendment 0 'Karuk Tribe' where state would be)
            return
        if disaster_number == 4147: # (Amendment 0 'Santa Clara Pueblo' where state would be)
            return
        if disaster_number == 4151: # (Amendment 1 'Santa Clara Pueblo' where state would be)
            return
        if disaster_number == 4206: # (Amendment 0 'Soboba Band of Luiseño Indians' where state would be)
            return
        if disaster_number == 4237: # (Amendment 0 'Oglala Sioux Tribe' where state would be)
            return
        if disaster_number == 4302: # (Amendment 0 'Hoopa Valley Tribe' where state would be)
            return
        if disaster_number == 4312: # (Amendment 0 'Resighini Rancheria' where state would be)
            return
        if disaster_number == 4341: # (Amendment 0 'Seminole Tribe of Florida' where state would be)
            return
        if disaster_number == 4352: # (Amendment 0 'Pueblo of Acoma' where state would be)
            return
        if disaster_number == 4384: # (Amendment 0 'Confederated Tribes of the Colville Reservat' where state would be)
            return
        if disaster_number == 4389: # (Amendment 0 'Havasupai Tribe' where state would be)
            return
        if disaster_number == 4409: # (Amendment 0 'Havasupai Tribe' where state would be)
            return
        if disaster_number == 4422: # (Amendment 0 'La Jolla Band of Luiseño Indians' where state would be)
            return
        if disaster_number == 4423: # (Amendment 0 'Cahuilla Band of Indians' where state would be)
            return
        if disaster_number == 4425: # (Amendment 0 'Soboba Band of Luiseño Indians' where state would be)
            return
        if disaster_number == 4430: # (Amendment 0 'The Sac & Fox Tribe of the Mississippi in Io' where state would be)
            return
        if disaster_number == 4436: # (Amendment 0 'Navajo Nation' where state would be)
            return
        if disaster_number == 4446: # (Amendment 0 'Ponca Tribe of Nebraska' where state would be)
            return
        if disaster_number == 4448: # (Amendment 0 'Oglala Sioux Tribe' where state would be)
            return
        if disaster_number == 4456: # (Amendment 0 'Muscogee (Creek) Nation' where state would be)
            return
        if disaster_number == 4545: # (Amendment 0 'Seminole Tribe of Florida' where state would be)
            return
        if disaster_number == 4561: # (Amendment 0 'Sac & Fox Tribe of the Mississippi in Iowa' where state would be)
            return
        if disaster_number == 4582: # (Amendment 0 'Navajo Nation' where state would be)
            return
        if disaster_number == 4591: # (Amendment 1 'Poarch Band of Creek Indians' where state would be)
            return
        
        ''' EXTRACT EFFECTIVE DATE '''
        #print(doc_text)
        print('\nPARSING:\t{a}-{b}'.format(a=disaster_number, b=amendment_number))

        date_match = re.search(self.regex['effective_date'], doc_text)
        if not date_match:
            date_match = re.search(self.regex['effective_date_var_2'], doc_text)
        
        if date_match:
            new_row['effectiveDate'] = date_match.group(1).strip()
        else:
            new_row['effectiveDate'] = 'SKIP: no date match'
            self.data.loc[len(self.data.index)] = new_row
            return


        ''' GET STATE '''
        #print(doc_text)
        state_match = re.search(self.regex['state'], doc_text)
        
        if state_match:
            print('STATE_MATCH:\t{}'.format(state_match))
            state_name = state_match.group().strip()
        if not state_match:
            state_match = re.search(self.regex['state_var_2'], doc_text)
            print('STATE_MATCH:\t{}'.format(state_match))
        if not state_match:
            state_match = re.search(self.regex['state_var_3'], doc_text)
            print('STATE_MATCH:\t{}'.format(state_match))
        if not state_match:
            state_match = re.search(self.regex['state_var_4'], doc_text)
            print('STATE_MATCH:\t{}'.format(state_match))
        if not state_match:
            new_row['GEOID'] = 'SKIP: no state match'
            

        state_name = state_match.group().strip(']')
        if 'Amendment' in state_name:
            state_name = state_name[:(state_name.index('Amendment'))].strip()
        #print(state_name)        
        supplementary_info_match = re.search(r'UPPLEMENTARY\s+INFORMATION', doc_text)
        supp_text = doc_text[supplementary_info_match.end():]


        
        # Get County Groups
        county_groups_iterable = re.finditer(self.regex['county_groups'], supp_text)
        county_groups = [i for i in county_groups_iterable]
        geoids_assist_catcode = self.handle_county_groups(county_groups, state_name, supp_text)
        
        for i in geoids_assist_catcode:
            new_row['GEOID'] = i['GEOID'][-5:]
            new_row['assistanceType'] = i['assistanceType']
            new_row['categoryCode'] = i['categoryCode']
            print(new_row)
            self.data.loc[len(self.data.index)] = new_row

        return

    def filter_county_groups(self, county_groups, state_name):
        cty_groups = county_groups

        out = []

        # for each match object
        for cty_group in cty_groups:
            there_are_valid_counties = False
            
            # grab list of individual possible county matches
            counties_iter = re.finditer(self.regex['individual_county'], cty_group.group())
            groups_counties = [i for i in counties_iter]
            
            # for each individual possible county match
            for county in groups_counties:
                temp = county.group(1)
                
                # clean it
                if "Count" in temp:
                    temp = temp[:temp.index("Count")].strip()
                if "count" in temp:
                    temp = temp[:temp.index("count")].strip()

                # if it is in the county data file
                if self.get_geoid(state_name, temp):
                    #print("HIT: " + temp)
                    there_are_valid_counties = True
            
            if there_are_valid_counties:
                out.append(cty_group) 
                #print(' ')
        
        return out

    # Takes in a slice of text that should contain
    def handle_county_group_mods(self, text):
        out = {
            "assistanceType": None,
            "categoryCode": None
        }

        # Get the assistanceType matches
        assistance_type_separated = self.extract_from_string(pattern=self.regex['assistance_type'], text=text)
        assistance_type = assistance_type_separated['extracted']
        converted_assistance_type = self.convert_assistance_type(assistance_type)
        out['assistanceType'] = converted_assistance_type
        
        # Get the categoryCode matches TODO: THIS NEEDS TO IGNORE THINGS LIKE 'already elligible for Categories A-B'
        category_code_separated = self.extract_from_string(pattern=self.regex['category_code'], text=text)
        category_code_matches = category_code_separated['extracted']
        converted_category_code_matches = self.convert_category_code(category_code_matches)
        out['categoryCode'] = converted_category_code_matches

        return out

    def handle_county_groups(self, county_groups, state_name, text):
        out = []
        
        cty_groups = self.filter_county_groups(county_groups, state_name)
        txt = text
        if 'CFDA' in txt:
            txt = txt[:txt.index('CFDA')]

        # when we have a reference to all counties we want to act accordingly
        all_counties = re.search(r'all count(ies/y)', text, re.IGNORECASE)
        if all_counties:
            pass #TODO:
        
        # we'll be consuming the last county group and the following to text
        # until we have the assistanceType and categoryCode phrases that
        # correspond to each group
        while len(cty_groups) > 0:
            target_group = cty_groups.pop(len(cty_groups)-1)
            target_text = txt[target_group.end():]
            # cut the text so that the modifying phrases that only refer to
            # this county group are not caught in future matching
            txt = txt[:target_group.start()]

            # find the assistanceType and categoryCode phrases ahead in the 
            # string that modify this county group
            group_mods = self.handle_county_group_mods(target_text)

            county_iter = re.finditer(self.regex['individual_county'], target_group.group(0))
            counties = [i.group(1) for i in county_iter]

            print('COUNTIES:\t{}'.format(counties))

            for i in counties:
                if self.get_geoid(state_name, i):
                    geoid = self.get_geoid(state_name, i)
                    out.append({
                        'GEOID': i+"-"+geoid,
                        'assistanceType': group_mods['assistanceType'],
                        'categoryCode': group_mods['categoryCode']
                    })

            geoids_in_out = [i['GEOID'] for i in out]
            there_are_duplicates_in_out = (len(set(geoids_in_out)) != len(geoids_in_out))
            
            self.merge_duplicates(out)

        return out
        
    # Takes two lists of dictionaries containing GEOID, assistanceType,
    # and categoryCode keys.
    def merge_duplicates(self, list_of_dicts):
        char_range = 'ABCDEFG'
        
        for i in list_of_dicts:
            # while there 
            if list_of_dicts.index(i)+1 < len(list_of_dicts):
                while i in list_of_dicts[(list_of_dicts.index(i)+1):]:
                    # take duplicate out of the list and save it to this variable
                    duplicate = list_of_dicts.pop(list_of_dicts[(list_of_dicts.index(i)+1):].index(i))
                    
                    # bitwise or operation on both of the assistanceTypes binary numbers in both dictionaries
                    first_num = str(int(i['assistanceType'][1]) | int(duplicate['assistanceType'][1]))
                    second_num = str(int(i['assistanceType'][2]) | int(duplicate['assistanceType'][2]))
                    i['assistanceType'] = i['assistanceType'][0] + first_num + second_num + i['assistanceType'][-1]

                    # combine categoryCode characters and then filter duplicate characters
                    temp = ''
                    pile_of_chars =  i['categoryCode'] + duplicate['categoryCode']
                    for char in char_range:
                        if char in pile_of_chars:
                            temp += char
                    # set the current dictionary
                    i['categoryCode'] = temp

        return







    ''' GEOID QUERY
    '''
    # Takes in two strings. One is the state name, one is the county name. 
    # Searches TIGERLine shape files for GEOIDs.
    # Returns the GEOID of the county.
    def get_geoid(self, state_name, county_name):
        # Use boolean arrays to find the row with the state name that we're looking for.
        state_data = self.state_geo_data
        temp = state_name.strip(';:') # shave off ':'s and ';'s
        #print(temp)
        # Handling some edge cases I found. May be temporary.
        if temp.strip() == "Commonwealth of Pennsylvania":
            temp = "Pennsylvania"
        if temp.strip() == "Commonwealth of Kentucky":
            temp = "Kentucky"
        if temp.strip() == "Northern Mariana Islands":
            temp = "Commonwealth of the Northern Mariana Islands"
        if temp.strip() == "Virgin Islands":
            temp = "United States Virgin Islands"
        if temp.strip() == "GUAM":
            temp = "Guam"
        
        if temp.strip() == "Federated States of Micronesia":
            return
        
        while "  " in temp.strip():
            temp = " ".join(temp.split())
        
        if "Count" in county_name:
            county_name = county_name[:county_name.index("Count")].strip()
        if "count" in county_name:
            county_name = county_name[:county_name.index("count")].strip()

        #print(temp)
        target_state = state_data.loc[state_data['NAME'] == temp.strip()]
        # Look up geoids in the boolean array. What's left is the state geoid we are looking for, so take the first thing
        state_geoid = target_state['GEOID'].array[0]
        

        # Do what we did before in the county dbf, but we filter by state firs. Accounts for duplicate names in counties
        # in other states.
        county_data = self.county_geo_data
        filtered_by_state = county_data.loc[county_data['STATEFP'] == state_geoid]
        
        target_county = filtered_by_state[filtered_by_state['NAME'] == county_name.strip()]
        
        if target_county['GEOID'].empty:
            return 
           
        return target_county['GEOID'].array[0]

    # Takes in assistance type mathes and converts then into the format we want
    # to be in the output file.
    def convert_assistance_type(self, at_from_doc):
        
        out = '00'

        if 'Individual Assistance' in at_from_doc:
            out = '1' + out[1:]
        if 'Public Assistance' in at_from_doc:
            out = out[:1] + '1' 
        """ + out[-1:]           <- this would go at the end of line above if adding hazard mitigation back (would need to make 'out' variable 3 zeros as well)
        if 'Hazard Mitigation' in at_from_doc:
            out = out[:2] + '1' """

        return '[' + out + ']' # Hacky way of making any preceding zeros stay

    # takes in two characters and returns those characters as well as all the in-between characters
    # in order and as a single string
    def range_char(self, start, stop):
        return ''.join(chr(n) for n in range(ord(start), ord(stop) + 1))

    # takes in the strings from category code regex matches and returns the corresponding letter(s)
    def convert_category_code(self, cc_from_doc):
        char_range = (chr(n) for n in range(ord('A'), ord('G') + 1))
        pile_of_characters = ''
        out = ''

        for i in cc_from_doc:
            
            matches = re.findall(self.regex['category_code_letters'], i)

            if len(matches) < 0:
                print('oops, convert_category_code-matches is too short')
                return
            if len(matches) == 0:
                return
            if len(matches) == 1:
                pile_of_characters += matches[0].upper()
            if len(matches) == 2:
                pile_of_characters += self.range_char(matches[0], matches[1]).upper()
            if len(matches) > 2:
                print('oops, convert_category_code-matches is too long')
                return pile_of_characters

        for char in char_range:
            if char in pile_of_characters:
                out += char

        return out

        




if __name__ == "__main__":
    
    # Instantiate Crawler Object
    crawler = FEMACrawler()

    # TESTING PARSE DOC ALONE 
    crawler.start_scrape([4548])
    

    