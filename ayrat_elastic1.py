# Import the requests module, so we can do the HTTP requests
import requests
import json

# Get the cleint name from the user
client_name = input("Please enter a client name: ")
client_name = client_name.upper()

# Elastic query is defined below
query = json.dumps({
    "size": "100000",
    "query": {
        "query_string": {
            "fields": ["customer.serviceContact.name","customer.billingContact.name"],
            "query": client_name,
            "default_operator": "OR"
        }
    }
})


# Send HTTP request
elastic_request = requests.get('http://alppapp086:9200/activecustomers/custdetails/_search', data = query )

# Get json response
json_response = elastic_request.json();

# Print response
output_file = open('ayrat_elastic1.csv','w')

header_line = "Customer Unique Id,Account Number,Service Name,Billing Name,\
               Service Street,Service City,Service State,Service Zip"
output_file.write(header_line + '\n')

customers = json_response['hits']['hits']
for customer in customers:
    output_line = '="'+customer['_source']['customer']['ezPayId']+'",'+ \
       '"'+customer['_source']['customer']['accountNumber']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['name']+'",'+ \
       '"'+customer['_source']['customer']['billingContact']['name']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['street']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['city']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['state']+'",'+ \
       '"'+customer['_source']['customer']['serviceContact']['address']['postalCode']+'"'
    output_file.write(output_line + '\n')