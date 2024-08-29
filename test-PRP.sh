#!/bin/bash 
# Test script for Party Routing Profle service domain 
#
# Coreless 4 data scenario defined 
# ref: https://biancoreteam.atlassian.net/wiki/download/attachments/4152492034/Coreless%20PoC%204%20-%20Data%20Flow.xlsx?api=v2
#
# Party Routing Profile	Status update based on insights outcome	
# Party Routing ID 	Customer 	Campaign Eligibility Flag
# --------------------- --------------- ------------------------------
# PRP_1			C1		NO
# PRP_2			C3		YES
# PRP_3			C4		YES		
# PRP_4			C5		NO

curl -X 'POST' 'http://localhost:5000/PartyRoutingProfile/Initiate' \
	-H 'accept: application/json' -H 'Content-Type: application/json' \
      	-d '{ "CustomerReference": "C1" }'

curl -X 'POST' 'http://localhost:5000/PartyRoutingProfile/Initiate' \
	-H 'accept: application/json' -H 'Content-Type: application/json' \
      	-d '{ "CustomerReference": "C3" }'

curl -X 'POST' 'http://localhost:5000/PartyRoutingProfile/Initiate' \
	-H 'accept: application/json' -H 'Content-Type: application/json' \
      	-d '{ "CustomerReference": "C4" }'

curl -X 'POST' 'http://localhost:5000/PartyRoutingProfile/Initiate' \
	-H 'accept: application/json' -H 'Content-Type: application/json' \
      	-d '{ "CustomerReference": "C5" }'

curl -X 'GET' 'http://localhost:5000/PartyRoutingProfile/1/Retrieve' \
	-H 'accept: application/json'

curl -X 'GET' 'http://localhost:5000/PartyRoutingProfile/2/Retrieve' \
	-H 'accept: application/json'

curl -X 'GET' 'http://localhost:5000/PartyRoutingProfile/3/Retrieve' \
	-H 'accept: application/json'

curl -X 'GET' 'http://localhost:5000/PartyRoutingProfile/4/Retrieve' \
	-H 'accept: application/json'

