# Import the required modules
import requests
import json
import sys
from functools import partial
from itertools import takewhile

# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
# Dictionary of all reserved characters that will be replaced
# Unable to add \ character to the below list - TODO
reps = {
'-':'\-',
'+':'\+',
'=':'\=',
'&&':'\&&',
'||':'\||',
'>':'\>',
'<':'\<',
'!':'\!',
'(':'\(',
')':'\)',
'{':'\{',
'}':'\}',
'[':'\[',
']':'\]',
'^':'\^',
'"':'\"',
'~':'\~',
'*':'\*',
'?':'\?',
':':'\:',
'/':'\/'}

# define our method
def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

def get_tokens(stream=sys.stdin):
    token = []
    for char in iter(partial(stream.read, 1), ''):
        if char.isspace(): # use any space as a separator
            if token:
                yield ''.join(token)
                del token[:]
        else:
            token.append(char)
    if token:
        yield ''.join(token)

print("Enter the State Names, followed by SPACE $ sign")
UserState = ' '.join(takewhile(lambda s: s != '$', get_tokens()))

print("Enter the City Names, followed by SPACE $ sign")
UserCity = ' '.join(takewhile(lambda s: s != '$', get_tokens()))

print("Enter the Street Address, followed by SPACE $ sign")
UserStreet = ' '.join(takewhile(lambda s: s != '$', get_tokens()))

#replace reserved characters
UserState = replace_all(UserState, reps)
UserCity = replace_all(UserCity, reps)
UserStreet = replace_all(UserStreet, reps)


query = json.dumps({
	"size": 100000,
    "query": {
        "bool": {
        "must": [
        {
           "multi_match": {
              "query": UserState,
			  "type": "cross_fields",
			  "operator": "or",
              "fields": ["customer.billingContact.address.state","customer.serviceContact.address.state"]
           }
        }],
       "should":[
        {
           "multi_match": {
              "query": UserCity,
			  "type": "cross_fields",
			  "operator": "or",
              "fields": ["customer.billingContact.address.city","customer.serviceContact.address.city"]
           }
        },		
        {
           "multi_match": {
              "query": UserStreet,
			  "type": "cross_fields",
			  "operator": "or",
              "fields": ["customer.billingContact.address.street","customer.billingContact.address.street2","customer.serviceContact.address.street","customer.serviceContact.address.street2" ]
           }
        }
        ],
        "minimum_should_match": "1"
      }
   }
})

# Send HTTP request
elastic_ActiveAddress_Response = requests.get('http://alppapp086:9200/activecustomers/custdetails/_search', data = query )

# Check response status code
#print(elastic_ActiveCust_Response.raise_for_status())
if elastic_ActiveAddress_Response.status_code != 200:
   print("No Response from Elastic Search engine for active customers")
   sys.exit()

# Send HTTP request
elastic_InActiveAddress_Response = requests.get('http://alppapp086:9200/inactivecustomers/custdetails/_search', data = query )

# Check response status code
#print(elastic_InActiveCust_Response.raise_for_status())
if elastic_InActiveAddress_Response.status_code != 200:
   print("No Response from Elastic Search engine for active customers")
   sys.exit()

# Get active users json response
active_json_response = elastic_ActiveAddress_Response.json();

# Get inactive users json response
inactive_json_response = elastic_InActiveAddress_Response.json();

print("Dumping data to Address_SearchResult.csv");
# Open file to write search result data.
output_file = open('Address_SearchResult.csv','w')

# Set header information of the CSV file
header_line = "Customer Unique Id,Account Number,Customer Status,Service Name,Billing Name,\
               Service Street,Service City,Service State,Service Zip"
output_file.write(header_line + '\n')

Acustomers = active_json_response['hits']['hits']
for customer in Acustomers:
    output_line = '="'+customer['_source']['customer']['ezPayId']+'",'+ \
       '"'+customer['_source']['customer']['accountNumber']+'",'+ \
	   '"Active",'+ \
       '"'+customer['_source']['customer']['serviceContact']['name']+'",'+ \
       '"'+customer['_source']['customer']['billingContact']['name']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['street']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['city']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['state']+'"'
    if customer['_source']['customer']['serviceContact']['address'].get('postalCode'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['postalCode']+'"'
    else:
       output_line = output_line +',"MIS-PCODE"'
    output_file.write(output_line + '\n')
	
IAcustomers = inactive_json_response['hits']['hits']
for bcustomer in IAcustomers:
    output_line = '="'+bcustomer['_source']['customer']['ezPayId']+'",'+ \
       '"'+bcustomer['_source']['customer']['accountNumber']+'",'+ \
	   '"InActive",'+ \
       '"'+bcustomer['_source']['customer']['serviceContact']['name']+'",'+ \
       '"'+bcustomer['_source']['customer']['billingContact']['name']+'",'+ \
       '"'+bcustomer['_source']['customer']['serviceContact']['address']['street']+'",'+ \
       '"'+bcustomer['_source']['customer']['serviceContact']['address']['city']+'",'+ \
       '"'+bcustomer['_source']['customer']['serviceContact']['address']['state']+'"'
    if customer['_source']['customer']['serviceContact']['address'].get('postalCode'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['postalCode']+'"'
    else:
       output_line = output_line +',"MIS-PCODE"'   
    output_file.write(output_line + '\n')

output_file.close()