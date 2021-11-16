import redis 
from data_connections import conn, call_function

response = call_function('get_mifid_number', 'gareth@upetrading.com')
print(response+2)