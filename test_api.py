import requests
import pytest

HOST = "localhost"
PORT = 5000


def test_cities():
  cleanup()

  nonexistent_city = get_city_raw("Vilnius")
  assert nonexistent_city.status_code == 404

  register_city("Vilnius", "Lithuania")
  register_city("Kaunas", "Lithuania")
  register_city("Riga", "Latvia")

  cities = get_cities()
  assert len(cities) == 3
  sorted_cities = sorted(cities, key=lambda x: x["name"])
  assert sorted_cities[0]["name"] == "Kaunas"
  assert sorted_cities[0]["country"] == "Lithuania"
  assert sorted_cities[1]["name"] == "Riga"
  assert sorted_cities[1]["country"] == "Latvia"
  assert sorted_cities[2]["name"] == "Vilnius"
  assert sorted_cities[2]["country"] == "Lithuania"

  cities = get_cities_in_country("Lithuania")
  assert len(cities) == 2
  sorted_cities = sorted(cities, key=lambda x: x["name"])
  assert sorted_cities[0]["name"] == "Kaunas"
  assert sorted_cities[0]["country"] == "Lithuania"
  assert sorted_cities[1]["name"] == "Vilnius"
  assert sorted_cities[1]["country"] == "Lithuania"



def test_airports():
  cleanup()

  nonexistent_airport = get_airport_raw("VNO")
  assert nonexistent_airport.status_code == 404

  register_city("Vilnius", "Lithuania")
  register_city("Kaunas", "Lithuania")
  register_city("Riga", "Latvia")

  register_airport("VNO", "Vilnius Airport", "Vilnius", 2, "Address 1")
  register_airport("KUN", "Kaunas Airport", "Kaunas", 1, "Address 2")
  register_airport("RIX", "Riga Airport", "Riga", 3, "Address 3")
  register_airport("EYVK", "Kyviškės Airport", "Vilnius", 1, "Address 4")

  airport = get_airport("VNO")
  assert airport["code"] == "VNO"
  assert airport["name"] == "Vilnius Airport"
  assert airport["city"] == "Vilnius"
  assert airport["numberOfTerminals"] == 2
  assert airport["address"] == "Address 1"

  airports = get_airports_in_a_city("Vilnius")
  assert len(airports) == 2

  airports_ordered = sorted(airports, key=lambda x: x["code"])
  assert airports_ordered[0]["code"] == "EYVK"
  assert airports_ordered[0]["name"] == "Kyviškės Airport"
  assert airports_ordered[0]["numberOfTerminals"] == 1
  assert airports_ordered[0]["address"] == "Address 4"
  assert airports_ordered[1]["code"] == "VNO"
  assert airports_ordered[1]["name"] == "Vilnius Airport"
  assert airports_ordered[1]["numberOfTerminals"] == 2
  assert airports_ordered[1]["address"] == "Address 1"

  kaunas_airports = get_airports_in_a_city("Kaunas")
  assert len(kaunas_airports) == 1

def test_flights():
  cleanup()

  nonexistent_flight = get_flight_raw("FR123")
  assert nonexistent_flight.status_code == 404

  register_city("Vilnius", "Lithuania")
  register_city("Kaunas", "Lithuania")
  register_city("Riga", "Latvia")
  register_city("Tallinn", "Estonia")

  register_airport("VNO", "Vilnius Airport", "Vilnius", 2, "Address 1")
  register_airport("KUN", "Kaunas Airport", "Kaunas", 1, "Address 2")
  register_airport("RIX", "Riga Airport", "Riga", 3, "Address 3")
  register_airport("EYVK", "Kyviškės Airport", "Vilnius", 1, "Address 4")
  register_airport("TLL", "Tallinn Airport", "Tallinn", 2, "Address 5")

  register_flight("FR123", "VNO", "KUN", 100, 60, "Ryanair")
  register_flight("FR124", "KUN", "RIX", 200, 120, "Ryanair")
  register_flight("FR125", "RIX", "VNO", 150, 90, "Ryanair")
  register_flight("FR126", "EYVK", "KUN", 50, 30, "Ryanair")
  register_flight("FR127", "RIX", "TLL", 300, 120, "Ryanair")

  flight = get_flight("FR123")
  assert flight["number"] == "FR123"
  assert flight["fromAirport"] == "VNO"
  assert flight["toAirport"] == "KUN"
  assert flight["price"] == 100
  assert flight["flightTimeInMinutes"] == 60
  assert flight["operator"] == "Ryanair"
  assert flight["fromCity"] == "Vilnius"
  assert flight["toCity"] == "Kaunas"

  flights = search_flights_between_cities("Vilnius", "Kaunas")
  assert len(flights) == 2

  flights_ordered = sorted(flights, key=lambda x: x["price"])
  assert flights_ordered[0]["fromAirport"] == "EYVK"
  assert flights_ordered[0]["toAirport"] == "KUN"
  assert flights_ordered[0]["flights"] == ["FR126"]
  assert flights_ordered[0]["price"] == 50
  assert flights_ordered[0]["flightTimeInMinutes"] == 30
  assert flights_ordered[1]["fromAirport"] == "VNO"
  assert flights_ordered[1]["toAirport"] == "KUN"
  assert flights_ordered[1]["flights"] == ["FR123"]
  assert flights_ordered[1]["price"] == 100
  assert flights_ordered[1]["flightTimeInMinutes"] == 60

  flights2 = search_flights_between_cities("Kaunas", "Tallinn")
  assert len(flights2) == 1

  flights2_ordered = sorted(flights2, key=lambda x: x["price"])
  assert flights2_ordered[0]["fromAirport"] == "KUN"
  assert flights2_ordered[0]["toAirport"] == "TLL"
  assert flights2_ordered[0]["flights"] == ["FR124", "FR127"]
  assert flights2_ordered[0]["price"] == 500
  assert flights2_ordered[0]["flightTimeInMinutes"] == 240
  

def register_city(name, country):
  url = f'http://{HOST}:{PORT}/cities'
  body = {"name": name, "country": country}
  response = requests.put(url, json=body)
  assert response.status_code == 204

def get_cities():
  url = f'http://{HOST}:{PORT}/cities'
  response = requests.get(url)
  assert response.status_code == 200
  return response.json()

def get_cities_in_country(country):
  url = f'http://{HOST}:{PORT}/cities?country={country}'
  response = requests.get(url)
  assert response.status_code == 200
  return response.json()

def delete_city(name):
  url = f'http://{HOST}:{PORT}/cities/{name}'
  response = requests.delete(url)
  assert response.status_code == 200

def get_city_raw(name):
  url = f'http://{HOST}:{PORT}/cities/{name}'
  response = requests.get(url)
  return response

def get_city(name):
  response = get_city_raw(name)
  assert response.status_code == 200
  return response.json()

def register_airport(code, name, city, numberOfTerminals, address):
  url = f'http://{HOST}:{PORT}/cities/{city}/airports'
  body = {
    "code": code,
    "name": name,
    "city": city,
    "numberOfTerminals": numberOfTerminals,
    "address": address
  }
  response = requests.put(url, json=body)
  assert response.status_code == 204

def get_airports_in_a_city(city):
  url = f'http://{HOST}:{PORT}/cities/{city}/airports'
  response = requests.get(url)
  assert response.status_code == 200
  return response.json()

def get_airport_raw(code):
  url = f'http://{HOST}:{PORT}/airports/{code}'
  response = requests.get(url)
  return response

def get_airport(code):
  response = get_airport_raw(code)
  assert response.status_code == 200
  return response.json()

def delete_airport(code):
  url = f'http://{HOST}:{PORT}/airports/{code}'
  response = requests.delete(url)
  assert response.status_code == 200

def register_flight(number, from_airport, to_airport, price, flight_time_in_minutes, operator):
  url = f'http://{HOST}:{PORT}/flights'
  body = {
    "number": number,
    "fromAirport": from_airport,
    "toAirport": to_airport,
    "price": price,
    "flightTimeInMinutes": flight_time_in_minutes,
    "operator": operator
  }
  response = requests.put(url, json=body)
  assert response.status_code == 204

def get_flight_raw(number):
  url = f'http://{HOST}:{PORT}/flights/{number}'
  response = requests.get(url)
  return response

def get_flight(number):
  response = get_flight_raw(number)
  assert response.status_code == 200
  return response.json()

def search_flights_between_cities(from_city, to_city):
  url = f'http://{HOST}:{PORT}/search/flights/{from_city}/{to_city}'
  response = requests.get(url)
  assert response.status_code == 200
  return response.json()

def cleanup():
  url = f'http://{HOST}:{PORT}/cleanup'
  response = requests.post(url)
  assert response.status_code == 200  
