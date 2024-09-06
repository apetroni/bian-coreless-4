# PartyRoutingProfile BIAN Service Domain 
# ref: https://app.swaggerhub.com/apis/BIAN-3/PartyRoutingProfile/12.0.0#/RetrieveAlertResponse
# Use Flask framework to implement a REST API Server

from flask import Flask, request
import json

app = Flask(__name__)
pst_id = 0
partyStateJson = '''
{   
    "PartyStateId": "",
    "CustomerReference": "",
    "Status": [
        {
            "StatusId": "",    
            "CustomerRelationshipStatusType": "",
            "CustomerRelationshipStatus": "",
            "CustomerRelationshipStatusNarrative": "",
            "CustomerRelationshipStatusValidFromToDate": ""
        }
    ],
    "Rating": [
        {
            "RatingId": "",
            "CustomerRelationshipRatingType": "",
            "CustomerRelationshipRating": "",
            "CustomerRelationshipRatingNarrative": "",
            "CustomerRelationPartyStateshipRatingValidFromToDate": ""
        }
    ],
    "Alert": [
        {   
            "AlertId": "",
            "CustomerRelationshipAlertType": "",
            "CustomerRelationshipAlert": "",
            "CustomerRelationshipAlertNarrative": "",
            "CustomerRelationshipAlertValidFromToDate": ""
        }
    ]
}
'''
# the internal list of party states
party_state_list = []
party_state_json = json.loads(partyStateJson)
    
def search_party_state_by_party_state_id(party_state_id) -> str: #json
    for party in party_state_list:
        res = party.get('PartyStateId',party_state_id)
        if res:
            if res == party_state_id:
                json_str = json.dumps(party)
                return json_str
    return None        
def search_party_state_by_customer_reference(customer_reference_id) -> str: #json
    for party in party_state_list:
        res = party.get('CustomerReference',customer_reference_id)
        if res:
            if res == customer_reference_id:
                json_str = json.dumps(party)
                return json_str
    return None        

# HTTP GET /PartyRoutingProfile/PartyState/{partystateid}/Retrieve
# (DDD Service) ReCR Retrieve details about the monitored party state            
@app.get('/PartyRoutingProfile/<party_state_id>/Retrieve')
def retrieve_CR_party_state(party_state_id):
    res = search_party_state_by_party_state_id(party_state_id) 
    if res:
        return res
    else:
        return "400"

# HTTP POST /PartyRoutingProfile/Initiate
# InCR Initiate profile state monitoring for a party
@app.post('/PartyRoutingProfile/Initiate')
def initiate_party_routing_profile_monitoring():
    body = request.get_json()
    customer_reference_id = body.get('CustomerReference')
    if not customer_reference_id:
        return "400" # invalid payload request
    res = search_party_state_by_customer_reference(customer_reference_id)
    if res:
        return f'CustomerRef {customer_reference_id} already monitored\n'
    
    # create a new CR party state for CustomerReference received
    new_party_state_json = party_state_json.copy()
    new_party_state_json['CustomerReference'] = customer_reference_id
    global pst_id
    pst_id += 1
    new_party_state_json['PartyStateId'] = str(pst_id)
    party_state_list.append(new_party_state_json)
    return f'CustomerRef added: {customer_reference_id}\n'    
#
#   INSERT INTO PartyRoutingProfile (CustomerReference) 
#       VALUES (@CustomerReference)

# Utility function: return the list of party states
@app.get('/PartyRoutingProfile/Dump')
def dump_party_routing_profile_list():
    return [party_routing_profile for party_routing_profile in party_state_list]


# HTTP POST /PartyRoutingProfile/{partyroutingprofileid}/Status/{statusid}/Update
# UpBQ Update status measures for a monitored party
# ref: https://app.swaggerhub.com/apis/BIAN-3/PartyRoutingProfile/12.0.0#/

@app.post('/PartyRoutingProfile/<party_routing_profile_id>/Status/<status_id>/Update')
def update_status_by_party_routing_profile_id_and_statusID(party_routing_profile_id,status_id):
    # search for Status item by StatusId for a given party routing profide and update state info
    # expected json payload
    payload = '''
            {
            "CustomerRelationshipStatusType": "Campaign Eligibility Flag",
            "CustomerRelationshipStatusNarrative": "Campaign Eligibility Flag Status",
            "CustomerRelationshipStatusValidFromToDate": "string"
            }
    '''
    return "200"
    # UPDATE State St 
    # WHERE St.StateId = @status_id
    # SET St.CustomerRelationshipStatusType = @CustomerRelationshipStatusType,
    #     St.CustomerRelationshipStatusNarrative = @CustomerRelationshipStatusNarrative,
    #     St.CustomerRelationshipStatusValidFromToDate = @CustomerRelationshipStatusValidFromToDate
     
    
# HTTP GET /PartyRoutingProfile/{partyroutingprofileid}/Status/{statusid}/Retrieve
# ReBQ Retrieve details about status measures for a party
# ref:https://app.swaggerhub.com/apis/BIAN-3/PartyRoutingProfile/12.0.0#/BQ%20-%20Status/RetrieveStatus
@app.get('/PartyRoutingProfile/<party_routing_profile_id>/Status/<status_id>/Retrieve')
def retrieve_status_by_party_routing_profile_id_and_status_id(party_routing_profile_id,status_id):
   # search for Status item by StatusId and return state info
    example_return_payload = '''
            {
            "CustomerRelationshipStatusType": "Campaign Eligibility Flag",
            "CustomerRelationshipStatusNarrative": "YES",
            "CustomerRelationshipStatusValidFromToDate": "2024-08-12T14:00:00Z"
            }
    '''
    return example_return_payload