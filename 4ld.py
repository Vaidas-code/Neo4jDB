from flask import Flask, request, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin123"))


def establish_relationships(session):
    session.run("""
        MATCH (a:Airport), (c:City)
        WHERE a.name CONTAINS c.name OR a.address CONTAINS c.name
        MERGE (a)-[:LOCATED_IN]->(c)
    """)

    session.run("""
        MATCH (f:Flight)-[:DEPARTS]->(a:Airport)-[:LOCATED_IN]->(c:City)
        MERGE (f)-[:DEPARTS_FROM_CITY]->(c)
    """)

    session.run("""
        MATCH (f:Flight)-[:ARRIVES]->(a:Airport)-[:LOCATED_IN]->(c:City)
        MERGE (f)-[:ARRIVES_IN_CITY]->(c)
    """)
#----------------------------------------------------------------
@app.route('/cities', methods=['PUT'])
def add_city():
    data = request.get_json()
    name = data.get('name')
    country = data.get('country')

    if not name or not country:
        return jsonify({
            "error": "Could not register the city, it exists or mandatory attributes are missing"
        }), 400

    with driver.session() as session:
        query = """
        MERGE (c:City {name: $name, country: $country})
        RETURN c.name AS city_name, c.country AS city_country
        """
        result = session.run(query, name=name, country=country)
        record = result.single()

        establish_relationships(session)

        return jsonify({
            "name": record["city_name"],
            "country": record["city_country"]
        }), 204


#---------------------------------------------------------------------------------
@app.route('/cities', methods=['GET'])
def get_cities():
    country = request.args.get('country')
    with driver.session() as session:
        query = """
        MATCH (c:City)
        WHERE $country IS NULL OR c.country = $country
        RETURN c.name AS city_name, c.country AS city_country
        """
        result = session.run(query, country=country)
        cities = [{"name": record["city_name"], "country": record["city_country"]} for record in result]
        return jsonify(cities), 200
#---------------------------------------------------------------------------------
@app.route('/cities/<string:name>', methods=['GET'])
def get_city_by_name(name):
    with driver.session() as session:
        query = """
        MATCH (c:City {name: $name})
        RETURN c.name AS city_name, c.country AS city_country
        """
        result = session.run(query, name=name)
        city = result.single()

        if not city:
            return jsonify({"message": "City not found"}), 404
        
        return jsonify({
            "name": city["city_name"],
            "country": city["city_country"]
        }), 200

#---------------------------------------------------------------------------------
@app.route('/cities/<string:name>/airports', methods=['PUT'])
def add_airport(name):
    data = request.get_json()

    code = data.get('code')
    airport_name = data.get('name')
    number_of_terminals = data.get('numberOfTerminals')
    address = data.get('address')

    if not all([code, airport_name, number_of_terminals, address]):
        return jsonify({"message": "Airport could not be created due to missing data or city the airport is registered in is not registered in the system"}), 400

    with driver.session() as session:
        city_query = "MATCH (c:City {name: $name}) RETURN c"
        city_result = session.run(city_query, name=name).single()
        if not city_result:
            return jsonify({"message": "City not found"}), 404

        session.run("""
        MATCH (c:City {name: $name})
        CREATE (a:Airport {code: $code, name: $airport_name, numberOfTerminals: $number_of_terminals, address: $address})
        MERGE (c)-[:HAS_AIRPORT]->(a)
        """, name=name, code=code, airport_name=airport_name, 
            number_of_terminals=number_of_terminals, address=address)

        session.run("""
        MATCH (a:Airport {code: $code}), (c:City {name: $name})
        MERGE (a)-[:LOCATED_IN]->(c)
        """, code=code, name=name)

        return jsonify({
            "code": code,
            "name": airport_name,
            "numberOfTerminals": number_of_terminals,
            "address": address
        }), 204


#---------------------------------------------------------------------------------
@app.route('/cities/<string:name>/airports', methods=['GET'])
def get_airports_in_city(name):
    with driver.session() as session:
        query = """
        MATCH (c:City {name: $name})-[:HAS_AIRPORT]->(a:Airport)
        RETURN a.code AS airport_code, c.name AS city_name, 
               a.name AS airport_name, a.numberOfTerminals AS terminals, 
               a.address AS airport_address
        """
        result = session.run(query, name=name)
        airports = result.data()

        if not airports:
            return jsonify({"message": "No airports found in the specified city"}), 404

        response = [
            {
                "code": airport["airport_code"],
                "city": airport["city_name"],
                "name": airport["airport_name"].strip(),
                "numberOfTerminals": airport["terminals"],
                "address": airport["airport_address"]
            }
            for airport in airports
        ]
        return jsonify(response), 200
#---------------------------------------------------------------------------------
@app.route('/airports/<string:code>', methods=['GET'])
def get_airport_by_code(code):
    with driver.session() as session:
        query = """
        MATCH (a:Airport {code: $code})-[:HAS_AIRPORT]-(c:City)
        RETURN a.code AS airport_code, c.name AS city_name, a.name AS airport_name, 
               a.numberOfTerminals AS number_of_terminals, a.address AS airport_address
        """
        result = session.run(query, code=code)
        airport = result.single()

        if not airport:
            return jsonify({"message": "City not found"}), 404

        return jsonify({
            "code": airport["airport_code"],
            "city": airport["city_name"],
            "name": airport["airport_name"].strip(),  
            "numberOfTerminals": airport["number_of_terminals"],
            "address": airport["airport_address"]
        }), 200


#---------------------------------------------------------------------------------
@app.route('/flights', methods=['PUT'])
def add_flight():
    data = request.get_json()

    flight_number = data.get('number')
    from_airport_code = data.get('fromAirport')
    to_airport_code = data.get('toAirport')
    price = data.get('price')
    flight_time_in_minutes = data.get('flightTimeInMinutes')
    operator = data.get('operator')

    if not all([flight_number, from_airport_code, to_airport_code, price, flight_time_in_minutes, operator]):
        return jsonify({"message": "Flight could not be created due to missing data"}), 400

    with driver.session() as session:
        result_from = session.run("MATCH (a:Airport {code: $code}) RETURN a", code=from_airport_code)
        result_to = session.run("MATCH (a:Airport {code: $code}) RETURN a", code=to_airport_code)

        if not result_from.single() or not result_to.single():
            return jsonify({"message": "Flight could not be created because one or both airports do not exist."}), 400

        result_flight = session.run("MATCH (f:Flight {number: $number}) RETURN f", number=flight_number)
        if result_flight.single():
            return jsonify({"message": "Flight with the given number already exists."}), 400

        query = """
        MERGE (f:Flight {number: $flight_number})
        ON CREATE SET f.price = $price, 
                      f.flightTimeInMinutes = $flight_time_in_minutes, 
                      f.operator = $operator
        MERGE (from:Airport {code: $from_code})
        MERGE (to:Airport {code: $to_code})
        MERGE (f)-[:DEPARTS_FROM]->(from)
        MERGE (f)-[:ARRIVES_AT]->(to)
        RETURN f.number AS flightNumber, from.code AS fromAirport, to.code AS toAirport
        """

        session.run(
            query,
            flight_number=flight_number,
            from_code=from_airport_code,
            to_code=to_airport_code,
            price=price,
            flight_time_in_minutes=flight_time_in_minutes,
            operator=operator
        )

    return jsonify({
        "number": flight_number,
        "fromAirport": from_airport_code,
        "toAirport": to_airport_code,
        "price": price,
        "flightTimeInMinutes": flight_time_in_minutes,
        "operator": operator
    }), 204


#---------------------------------------------------------------------------------
@app.route('/flights/<string:number>', methods=['GET'])
def get_flight(number):
    with driver.session() as session:
        query = """
        MATCH (f:Flight {number: $number})-[:DEPARTS_FROM]->(fromAirport:Airport)-[:LOCATED_IN]->(fromCity:City),
            (f)-[:ARRIVES_AT]->(toAirport:Airport)-[:LOCATED_IN]->(toCity:City)
        RETURN f, fromAirport, fromCity, toAirport, toCity
        """

        result = session.run(query, number=number)
        flight_data = result.single()

        if not flight_data:
            return jsonify({"error": "Flight not found."}), 404

        flight_node = flight_data['f']
        from_airport_node = flight_data['fromAirport']
        from_city_node = flight_data['fromCity']
        to_airport_node = flight_data['toAirport']
        to_city_node = flight_data['toCity']

        flight = {
            "number": flight_node["number"],
            "fromAirport": from_airport_node["code"],
            "fromCity": from_city_node["name"],
            "toAirport": to_airport_node["code"],
            "toCity": to_city_node["name"],
            "price": flight_node["price"],
            "flightTimeInMinutes": flight_node["flightTimeInMinutes"],
            "operator": flight_node["operator"]
        }
        
        return jsonify(flight), 200


#-----------------------------------------------------------------------------
@app.route('/search/flights/<string:fromcity>/<string:tocity>', methods=['GET'])
def get_flights_between_cities(fromcity, tocity):
    try:
        with driver.session() as session:
            from_city_check_query = """
            MATCH (from:City {name: $fromcity})<-[:LOCATED_IN]-(fromAirport:Airport)-[:DEPARTS_FROM]->(f:Flight)
            RETURN COUNT(f) AS flightCount
            """
            from_city_check_result = session.run(from_city_check_query, fromcity=fromcity)
            from_flight_count = from_city_check_result.single()["flightCount"]

            if from_flight_count == 0:
                return jsonify({"error": f"No flights found departing from {fromcity}."}), 404

            to_city_check_query = """
            MATCH (to:City {name: $tocity})<-[:LOCATED_IN]-(toAirport:Airport)-[:ARRIVES_AT]->(f:Flight)
            RETURN COUNT(f) AS flightCount
            """
            to_city_check_result = session.run(to_city_check_query, tocity=tocity)
            to_flight_count = to_city_check_result.single()["flightCount"]

            if to_flight_count == 0:
                return jsonify({"error": f"No flights arriving at {tocity}."}), 404

            query = """
            MATCH path = (from:City {name: $fromcity})<-[:LOCATED_IN]-(fromAirport:Airport)<-[:DEPARTS_FROM]-(f1:Flight)-[:ARRIVES_AT]->(toAirport:Airport)-[:LOCATED_IN]->(to:City {name: $tocity})
            WITH path, 
                 [n IN nodes(path) WHERE n:Flight | n.number] AS flightNumbers,
                 [n IN nodes(path) WHERE n:Flight | n.price] AS prices,
                 [n IN nodes(path) WHERE n:Flight | n.flightTimeInMinutes] AS flightTimes,
                 [n IN nodes(path) WHERE n:Airport | n.code] AS airports
            RETURN 
                airports[0] AS fromAirport,
                airports[-1] AS toAirport,
                flightNumbers AS flights,
                reduce(totalPrice = 0, price IN prices | totalPrice + price) AS price,
                reduce(totalTime = 0, time IN flightTimes | totalTime + time) AS flightTime
            ORDER BY price
            """
            
            result = session.run(query, fromcity=fromcity, tocity=tocity)
            flights = []
            for record in result:
                flight = {
                    "fromAirport": record["fromAirport"],
                    "toAirport": record["toAirport"],
                    "flights": record["flights"],
                    "price": record["price"],
                    "flightTimeInMinutes": record["flightTime"]
                }
                flights.append(flight)

            return jsonify(flights), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
#---------------------------------------------------------------------------------
@app.route('/cleanup', methods=['POST'])
def cleanup():
    with driver.session() as session:
        query = """
        MATCH (n)
        DETACH DELETE n
        """
        session.run(query)

    return jsonify({"message": "Cleanup Successful"}), 200
#---------------------------------------------------------------------------------


if __name__ == '__main__':
    app.run(debug=True)
