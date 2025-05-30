data injestion from api which has pagination
 
Source: REST API with pagination.
Destination: File or Database
 
handle 3 cases for code
1. if no api response
2. if api response empty(show all data)
3. if pagination over/no pagination(show all data)
 
#test-case
source and destination data count matches then pass otherwise fail