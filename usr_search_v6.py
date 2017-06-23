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

print("Enter the user names to search, followed by SPACE $ sign")
UserSearchString = ' '.join(takewhile(lambda s: s != '$', get_tokens()))

#replace reserved characters
UserSearchString = replace_all(UserSearchString, reps)

#print(UserSearchString)


query = json.dumps({
    "size": 100000,
    "_source": [ "customer.ezPayId","customer.accountNumber","customer.serviceContact.contactName","customer.serviceContact.name","customer.billingContact.name","customer.serviceContact.address.*"],
    "query": {
    "bool": {
      "should": [
        { "query_string": {"query": UserSearchString} },
        { "regexp":{ "customer.serviceContact.name": UserSearchString+".<0-9>"} },
        { "regexp":{ "customer.serviceContact.contactName": UserSearchString+".<0-9>"} },
        { "regexp":{ "customer.billingContact.name": UserSearchString+".<0-9>"} }
      ]
    }
    },
	"filter":{
	"or": [
		{ 
			"term": { "customer.serviceContact.name": [UserSearchString] }
		},
		{ 
			"term": { "customer.serviceContact.contactName": [UserSearchString] }
		},
		{
		        "term": { "customer.billingContact.name": [UserSearchString] }
		}
		]
	}
})

print(query)

# Send HTTP request
elastic_ActiveCust_Response = requests.get('http://ES_IPADDRESS:9202/activecustomers/custdetails/_search', data = query )

# Check response status code
#print(elastic_ActiveCust_Response.raise_for_status())
if elastic_ActiveCust_Response.status_code != 200:
   print("No Response from Elastic Search engine for active customers")
   sys.exit()

# Send HTTP request
elastic_InActiveCust_Response = requests.get('http://ES_IPADDRESS:9202/inactivecustomers/custdetails/_search', data = query )

# Check response status code
#print(elastic_InActiveCust_Response.raise_for_status())
if elastic_InActiveCust_Response.status_code != 200:
   print("No Response from Elastic Search engine for active customers")
   sys.exit()

# Get active users json response
active_json_response = elastic_ActiveCust_Response.json();

# Get inactive users json response
inactive_json_response = elastic_InActiveCust_Response.json();

print("Dumping data to NewIP_User_SearchResult_V6.csv");
# Open file to write search result data.
output_file = open('NewIP_User_SearchResult_V6.csv','w')

# Set header information of the CSV file
header_line = "Customer Unique Id,Account Number,Customer Status,Service ContactName,Service Name, Billing Name,\
               Service State,Service City,Service Street,Service Zip"
output_file.write(header_line + '\n')

Acustomers = active_json_response['hits']['hits']
#json.dump(Acustomers, open('activeusrs.json','w'))
for customer in Acustomers:
    output_line = '="'+customer['_source']['customer']['ezPayId']+'",'+ \
       '"'+customer['_source']['customer']['accountNumber']+'",'+ \
       '"Active"'
    if customer['_source']['customer']['serviceContact'].get('contactName'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['contactName']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['serviceContact'].get('name'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['name']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['billingContact'].get('name'):
       output_line = output_line +',"'+customer['_source']['customer']['billingContact']['name']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['serviceContact']['address'].get('state'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['state']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['serviceContact']['address'].get('city'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['city']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['serviceContact']['address'].get('street'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['street']+'"'
    else:
       output_line = output_line +'," "'
    if customer['_source']['customer']['serviceContact']['address'].get('postalCode'):
       output_line = output_line +',"'+customer['_source']['customer']['serviceContact']['address']['postalCode']+'"'
    else:
       output_line = output_line +'," "'
    output_file.write(output_line + '\n')


IAcustomers = inactive_json_response['hits']['hits']
#json.dump(bcustomers, open('inactiveusrs.json','w'))
for bcustomer in IAcustomers:
    boutput_line = '="'+bcustomer['_source']['customer']['ezPayId']+'",'+ \
       '"'+bcustomer['_source']['customer']['accountNumber']+'",'+ \
       '"InActive"'
    if bcustomer['_source']['customer']['serviceContact'].get('contactName'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['contactName']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['serviceContact'].get('name'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['name']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['billingContact'].get('name'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['billingContact']['name']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['serviceContact']['address'].get('state'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['address']['state']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['serviceContact']['address'].get('city'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['address']['city']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['serviceContact']['address'].get('street'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['address']['street']+'"'
    else:
       boutput_line = boutput_line +'," "'
    if bcustomer['_source']['customer']['serviceContact']['address'].get('postalCode'):
       boutput_line = boutput_line +',"'+bcustomer['_source']['customer']['serviceContact']['address']['postalCode']+'"'
    else:
       boutput_line = boutput_line +'," "'
    output_file.write(boutput_line + '\n')

output_file.close()