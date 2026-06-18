import os
import requests

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
AVIATIONSTACK_API_KEY = os.getenv("AVIATIONSTACK_API_KEY")

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

params = {
    "access_key": AVIATIONSTACK_API_KEY
}

def get_flights_data(endpoint):
    url = f"http://api.aviationstack.com/v1/{endpoint}"
    
    api_result = requests.get(url, params=params)
    api_response = api_result.json()
    flights = api_response['data']

    for flight in flights:
        flight_data = dict()
        flight_data['aircraft_iata'] = flight['aircraft']['iata'] if flight['aircraft'] else None
        flight_data['aircraft_registration'] = flight['aircraft']['registration'] if flight['aircraft'] else None
        flight_data['airline_icao'] = flight['airline']['icao'] if flight['airline'] else None
        flight_data['arrival_actual'] = flight['arrival']['actual'] if flight['arrival'] else None
        flight_data['arrival_baggage'] = flight['arrival']['baggage'] if flight['arrival'] else None
        flight_data['arrival_delay'] = flight['arrival']['delay'] if flight['arrival'] else None
        flight_data['arrival_gate'] = flight['arrival']['gate'] if flight['arrival'] else None
        flight_data['arrival_iata'] = flight['arrival']['iata'] if flight['arrival'] else None
        flight_data['arrival_icao'] = flight['arrival']['icao'] if flight['arrival'] else None
        flight_data['arrival_scheduled'] = flight['arrival']['scheduled'] if flight['arrival'] else None
        flight_data['arrival_terminal'] = flight['arrival']['terminal'] if flight['arrival'] else None
        flight_data['departure_actual'] = flight['departure']['actual'] if flight['departure'] else None
        flight_data['departure_delay'] = flight['departure']['delay'] if flight['departure'] else None
        flight_data['departure_gate'] = flight['departure']['gate'] if flight['departure'] else None
        flight_data['departure_iata'] = flight['departure']['iata'] if flight['departure'] else None
        flight_data['departure_icao'] = flight['departure']['icao'] if flight['departure'] else None
        flight_data['departure_scheduled'] = flight['departure']['scheduled'] if flight['departure'] else None
        flight_data['departure_terminal'] = flight['departure']['terminal'] if flight['departure'] else None
        flight_data['flight_date'] = flight['flight_date']
        flight_data['flight_iata'] = flight['flight']['iata'] if flight['flight'] else None
        flight_data['flight_icao'] = flight['flight']['icao'] if flight['flight'] else None
        flight_data['flight_number'] = flight['flight']['number'] if flight['flight'] else None
        flight_data['flight_status'] = flight['flight_status']
        flight_data['live_altitude'] = flight['live']['altitude'] if flight['live'] else None
        flight_data['live'] = flight['live']['is_ground'] if flight['live'] else None
        producer.send('flights', value=str(flight_data).encode('utf-8'))
        producer.flush()
        print("Sent flight data to Kafka......")
    
    return

def get_airplanes_data(endpoint):
    url = f"http://api.aviationstack.com/v1/{endpoint}"

    api_result = requests.get(url, params=params)
    api_response = api_result.json()
    airplanes = api_response['data']

    for airplane in airplanes:
        airplane_data = dict()
        airplane_data['airplane_id'] = airplane['aircraft_iata']
        airplane_data['construction_number'] = airplane['construction_number']
        airplane_data['delivery_date'] = airplane['delivery_date']
        airplane_data['engine_count'] = airplane['engine_count']
        airplane_data['engine_type'] = airplane['engine_type']
        airplane_data['first_flight_date'] = airplane['first_flight_date']
        airplane_data['iata_type'] = airplane['iata_type']
        airplane_data['model_code'] = airplane['model_code']
        airplane_data['model_name'] = airplane['model_name']
        airplane_data['plane_age'] = airplane['plane_age']
        airplane_data['plane_owner'] = airplane['plane_owner']
        airplane_data['plane_series'] = airplane['plane_series']
        airplane_data['plane_status'] = airplane['plane_status']
        airplane_data['production_line'] = airplane['production_line']
        airplane_data['registration_number'] = airplane['registration_number']
        producer.send('airplanes', value=str(airplane_data).encode('utf-8'))
        producer.flush()
        print("Sent airplane data to Kafka......")
    
    return