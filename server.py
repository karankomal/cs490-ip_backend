from flask import Flask, jsonify, request, make_response
from flask_paginate import Pagination
from flask_mysqldb import MySQL



app = Flask(__name__)
mysql = MySQL()
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'sakila'

mysql.init_app(app)

# Home
@app.route("/allfilms")
def film():
    cursor = mysql.connection.cursor()
    query = """SELECT film.film_id, initcap(film.title) as title, category.name, GROUP_CONCAT(initcap(actor.first_name), ' ' , initcap(actor.last_name) SEPARATOR ', ') as actors FROM film
	            JOIN film_category ON film.film_id = film_category.film_id
	            JOIN category ON category.category_id = film_category.category_id
                JOIN film_actor ON film.film_id = film_actor.film_id
                JOIN actor ON film_actor.actor_id = actor.actor_id
	            GROUP BY film.film_id, title, category.name ORDER BY film.film_id;"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify({'films': result})

# View All Films - Pagination
@app.route("/films", methods=["GET"])
def films():
    cursor = mysql.connection.cursor()
    query = """SELECT film.film_id, film.title, category.name FROM film
                JOIN film_category ON film.film_id = film_category.film_id
                JOIN category ON category.category_id = film_category.category_id
                ORDER BY film.film_id"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=10, type=int)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    paginated_films = result[start_index:end_index]    
    return jsonify({'films': paginated_films})

# Top 5 Rented Movies
@app.route("/top5Rented")
def top5Rented():
    cursor = mysql.connection.cursor()
    query = """SELECT film.film_id, initcap(film.title) as title, category.name as category, COUNT(*) as rented FROM rental
                JOIN inventory ON rental.inventory_id = inventory.inventory_id
                JOIN film ON inventory.film_id = film.film_id
	            JOIN film_category ON film.film_id = film_category.film_id
                JOIN category ON category.category_id = film_category.category_id
                GROUP BY inventory.film_id, category.name ORDER BY (count(*)) DESC LIMIT 5"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# Film Details Based On ID
@app.route("/film/<id>")
def filmDetails(id):
    cursor = mysql.connection.cursor()
    query = """SELECT film.film_id, initcap(film.title) as title, film.description, film.release_year, film.language_id, film.original_language_id, film.rental_duration, film.rental_rate, film.length, film.replacement_cost, film.rating, film.special_features, film.last_update, GROUP_CONCAT(initcap(actor.first_name), ' ' , initcap(actor.last_name) SEPARATOR ', ') as actors FROM film 
                JOIN film_actor ON film.film_id = film_actor.film_id
                JOIN actor ON film_actor.actor_id = actor.actor_id
                WHERE film.film_id = {}
                GROUP BY film.film_id, title, film.description, film.release_year, film.language_id, film.original_language_id, film.rental_duration, film.rental_rate, film.length, film.replacement_cost, film.rating, film.special_features, film.last_update""".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# Top 5 Actors Based On Rentals
@app.route("/top5Actors")
def top5Actors():
    cursor = mysql.connection.cursor()
    query = """SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(*) as movies FROM actor
	            JOIN film_actor on actor.actor_id = film_actor.actor_id
	            GROUP BY actor.actor_id ORDER BY (COUNT(*)) DESC LIMIT 5;"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# Actor Details Based On ID
@app.route("/actor/<id>")
def actorDetails(id):
    cursor = mysql.connection.cursor()
    query = "SELECT * from actor WHERE actor_id = {}".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# Top 5 Movies for an actor based on ID
@app.route("/top5/actor/<id>")
def top5MoviesActor(id):
    cursor = mysql.connection.cursor()
    query = """SELECT film.film_id, initcap(film.title) as title, category.name as category, COUNT(*) as rented FROM rental
	            JOIN inventory ON rental.inventory_id = inventory.inventory_id
	            JOIN film ON inventory.film_id = film.film_id
	            JOIN film_category ON film.film_id = film_category.film_id
	            JOIN category ON category.category_id = film_category.category_id
                JOIN film_actor ON film_actor.film_id = film.film_id AND film_actor.actor_id = {}
	            GROUP BY inventory.film_id, category.name ORDER BY (count(*)) DESC LIMIT 5;""".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# View All Customers
@app.route("/allcustomers")
def customer():
    cursor = mysql.connection.cursor()
    query = """SELECT customer.customer_id, customer.store_id, customer.first_name, customer.last_name, customer.email, customer.address_id, customer.active, customer.create_date, customer.last_update, address.address, address.address2, address.district, address.city_id, address.postal_code, address.phone, city.city, city.country_id, country.country FROM customer
                JOIN address on customer.address_id = address.address_id
                JOIN city ON address.city_id = city.city_id
                JOIN country ON city.country_id = country.country_id
                ORDER BY customer_id;"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# View All Customers - Pagination
@app.route("/customers", methods=["GET"])
def customers():
    cursor = mysql.connection.cursor()
    query = """SELECT * from customer
                ORDER BY customer_id"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=10, type=int)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    paginated_customers = result[start_index:end_index]    
    return jsonify({'customers': paginated_customers})

# View Customer Details based on ID
@app.route("/customer/<id>")
def customerDetails(id):
    cursor = mysql.connection.cursor()
    query = """SELECT customer.customer_id, customer.store_id, customer.first_name, customer.last_name, customer.email, customer.address_id, customer.active, customer.create_date, customer.last_update, address.address, address.address2, address.district, address.city_id, address.postal_code, address.phone, city.city, city.country_id, country.country FROM customer
                JOIN address on customer.address_id = address.address_id
                JOIN city ON address.city_id = city.city_id
                JOIN country ON city.country_id = country.country_id
                WHERE customer_id = {}""".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# View Customers' Previously Rented History
@app.route("/customer/<id>/previouslyrented")
def previouslyrented(id):
    cursor = mysql.connection.cursor()
    query = """SELECT customer.customer_id, COUNT(*) as count FROM customer
                JOIN rental ON customer.customer_id = rental.customer_id
                WHERE return_date IS NOT NULL AND customer.customer_id = {}
                GROUP BY customer.customer_id ORDER BY COUNT(*) DESC""".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return (jsonify(result))

# View Customers' Currently Renting History
@app.route("/customer/<id>/currentlyrenting")
def currentlyrenting(id):
    cursor = mysql.connection.cursor()
    query = """SELECT customer.customer_id, COUNT(*) as count FROM customer
	            JOIN rental ON customer.customer_id = rental.customer_id
	            WHERE return_date IS NULL AND customer.customer_id = {}
	            GROUP BY customer.customer_id ORDER BY COUNT(*) DESC""".format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return (jsonify(result))

# Rent Film Out to Customer
@app.route("/rentfilm", methods=['POST'])
def rentfilm():
    conn = mysql.connection

    data = request.get_json()
    custID = data[0]['customer_id']
    filmID = data[1]['film_id']
    
    # Make sure customer ID exists
    if custID == "":
        response = make_response("Error, Customer DNE")
        response.headers["error"] = "Customer DNE"
        response.status_code = 400
        return response

    cursor = conn.cursor()
    query = "SELECT customer_id FROM customer WHERE customer_id = {}".format(custID)
    cursor.execute(query)
    row = cursor.fetchone()
    if row == None:
        response = make_response("Error, Customer DNE")
        response.headers["error"] = "Customer DNE"
        response.status_code = 400
        return response
    
    # Make sure movie is in stock
    query = "CALL film_in_stock({}, 1, @c);".format(filmID)
    cursor.execute(query)
    stock = cursor.fetchall()
    print(stock)
    print(len(stock))
    if len(stock) < 1:
        response = make_response("Error, Out of Stock")
        response.headers["error"] = "Out of Stock"
        response.status_code = 400
        return response
    
    # Make sure customer has no outstanding balance
    query = "SELECT get_customer_balance({}, NOW())".format(custID)
    cursor.execute(query)
    balance = cursor.fetchall()
    if balance[0][0] > 0.00:
        response = make_response("Error, Outstanding Balance")
        response.headers["error"] = "Outstanding Balance"
        response.status_code = 400
        return response
    
    # If all checks pass, rent movie to customer.
    inventID = stock[0][0]
    query = """INSERT INTO rental(rental_date, inventory_id, customer_id, staff_id, last_update) 
                            VALUES(NOW(), {}, {}, 1, NOW());""".format(inventID, custID)
    cursor.execute(query)
    conn.commit()
    
    cursor.close()
    return 'Done', 200

# Customer is Returning a Rental
@app.route("/returnfilm", methods=['POST'])
def returnfilm():
    conn = mysql.connection
    data = request.get_json()
    custID = data[0]['customer_id']
    filmID = data[1]['film_id']
    
    # Make sure film ID exists
    if filmID == "":
        response = make_response("Error, Film Not Entered")
        response.headers["error"] = "Film Not Entered"
        response.status_code = 400
        return response

    cursor = conn.cursor()
    query = "SELECT film_id FROM film WHERE film_id = {}".format(filmID)
    cursor.execute(query)
    row = cursor.fetchone()
    if row == None:
        response = make_response("Error, Film DNE")
        response.headers["error"] = "Film DNE"
        response.status_code = 400
        return response
    
    # Find valid rental and inventory IDs (if they exist)
    cursor = conn.cursor()
    query = """SELECT rental.rental_id, rental.inventory_id FROM 
                (SELECT inventory.inventory_id FROM inventory
                    JOIN film ON film.film_id = {} AND inventory.film_id = film.film_id) AS validInventories
                JOIN rental on rental.inventory_id = validInventories.inventory_id AND rental.customer_id = {} AND rental.return_date IS NULL LIMIT 1;""".format(filmID, custID)
    cursor.execute(query)
    rentalAndInventoryIDs = cursor.fetchone()
    if rentalAndInventoryIDs == None:
        response = make_response("Error, Film Not Rented")
        response.headers["error"] = "Film Not Rented"
        response.status_code = 400
        return response
    
    rentalID = rentalAndInventoryIDs[0]
    inventoryID = rentalAndInventoryIDs[1]

    # Complete rental return and update inventory to be at store 1.
    query = "UPDATE rental SET return_date = NOW() WHERE rental_id = {};".format(rentalID)
    cursor.execute(query)
    query = "UPDATE inventory SET store_id = 1 WHERE inventory_id = {};".format(inventoryID)
    cursor.execute(query)
    conn.commit()

    cursor.close()
    return 'Done', 200

# Create A New Customer
@app.route("/addcustomer", methods=['POST'])
def addcustomer():
    conn = mysql.connection
    data = request.get_json()
    firstName = data[0]['first_name']
    lastName = data[1]['last_name']
    email = data[2]['email']
    address = data[3]['address']
    address2 = data[4]['address2']
    city = data[5]['city']
    district = data[6]['district']
    country = data[7]['country']
    postal_code = data[8]['postal_code']
    phone = data[9]['phone']

    # Make sure inputs are not invalid
    if (firstName == "" or lastName == "" or email == "" or address == "" or district == "" or postal_code == "" or city == "" or country == "" or phone == ""):
        response = make_response("Error, Fields Invalid")
        response.headers["error"] = "Invalid Fields"
        response.status_code = 400
        return response
    
    # Make sure email doesn't already exist
    cursor = conn.cursor()
    query = "SELECT email FROM customer WHERE email = %s"
    cursor.execute(query, (email,))
    emailExists = cursor.fetchall()
    if len(emailExists) != 0:
        response = make_response("Error, Email Already Exists")
        response.headers["error"] = "Email Exists"
        response.status_code = 400
        return response
    
    # Check if address already exists
    query = """SELECT address.address_id, city.city_id, country.country_id FROM address 
        JOIN city ON address.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id
        WHERE address.address = %s AND address.address2 = %s AND address.district = %s AND address.postal_code = %s AND city.city = %s AND country = %s;"""
    cursor.execute(query, (address, address2, district, postal_code, city, country,))
    addressExists = cursor.fetchall()
    if (len(addressExists) != 0):
        response = make_response("Error, Address Already Exists")
        response.headers["error"] = "Address Exists"
        response.status_code = 400
        return response

    # Check if country already exists and if so store it in variable. Else, add it and store ID.
    query = "SELECT country_id FROM country WHERE country = %s;"
    cursor.execute(query, (country,))
    countryExists = cursor.fetchall()
    countryID = ""
    if (len(countryExists) != 0):
        countryID = countryExists[0][0]
    else: 
        query = "INSERT INTO country(country, last_update) VALUES (%s, NOW())"
        cursor.execute(query, (country,))
        query = "SELECT country_id FROM country WHERE country = %s;"
        cursor.execute(query, (country,))
        countryID = cursor.fetchall()[0][0]

    # Check if city already exists and if so store it in variable. Else, add it and store ID.
    query = "SELECT city_id FROM city WHERE city = %s;"
    cursor.execute(query, (city,))
    cityExists = cursor.fetchall()
    cityID = ""
    if (len(cityExists) != 0):
        cityID = cityExists[0][0]
    else: 
        query = "INSERT INTO city(city, country_id, last_update) VALUES (%s, %s NOW())"
        cursor.execute(query, (city, countryID,))
        query = "SELECT city_id FROM city WHERE city = %s;"
        cursor.execute(query, (city,))
        cityID = cursor.fetchall()[0][0]
    
    # Add new address and store address ID
    query = """INSERT INTO address(address, address2, district, city_id, postal_code, phone, location, last_update)
                VALUES(%s, %s, %s, %s, %s, %s, POINT(1,1), NOW());"""
    cursor.execute(query, (address, address2, district, cityID, postal_code, phone,))
    query = "SELECT address_id FROM address WHERE address = %s AND address2 = %s AND district = %s AND postal_code = %s;"
    cursor.execute(query, (address, address2, district, postal_code,))
    addressID = cursor.fetchall()[0][0]
    
    # Finally, Add New Customer
    query = """INSERT INTO customer(store_id, first_name, last_name, email, address_id, active, create_date, last_update)
                         VALUES(1, %s, %s, %s, %s, 1, NOW(), NOW())"""
    cursor.execute(query, (firstName, lastName, email, addressID,))
    conn.commit()

    cursor.close()
    return 'Done', 200

# Edit Existing Customer
@app.route("/editcustomer", methods=['PATCH'])
def editcustomer():
    conn = mysql.connection
    data = request.get_json()
    firstName = data[0]['first_name']
    lastName = data[1]['last_name']
    email = data[2]['email']
    address = data[3]['address']
    address2 = data[4]['address2']
    city = data[5]['city']
    district = data[6]['district']
    country = data[7]['country']
    postal_code = data[8]['postal_code']
    phone = data[9]['phone']
    custID = data[10]['customer_id']
    addressID = data[11]['address_id']
    cityID = data[12]['city_id']
    countryID = data[13]['country_id']

    # Make sure inputs are not invalid
    if (firstName == "" or lastName == "" or email == "" or address == "" or district == "" or postal_code == "" or city == "" or country == "" or phone == ""):
        response = make_response("Error, Fields Invalid")
        response.headers["error"] = "Invalid Fields"
        response.status_code = 400
        return response
    
    # Check if country changed. If so, need new country ID,
    cursor = conn.cursor()
    query = "SELECT country FROM country WHERE country_id = %s;"
    cursor.execute(query, (countryID,))
    oldCountry = cursor.fetchall()[0][0]
    newCountryID = ""
    if (oldCountry == country):
        newCountryID = countryID
    else:
        # Check if new country already exists and if so store it in variable. Else, add it and store ID.
        query = "SELECT country_id FROM country WHERE country = %s;"
        cursor.execute(query, (country,))
        countryExists = cursor.fetchall()
        if (len(countryExists) != 0):
            newCountryID = countryExists[0][0]
        else: 
            query = "INSERT INTO country(country, last_update) VALUES (%s, NOW())"
            cursor.execute(query, (country,))
            query = "SELECT country_id FROM country WHERE country = %s;"
            cursor.execute(query, (country,))
            newCountryID = cursor.fetchall()[0][0]
    
    # Check if city changed. If so, need new city ID.
    query = "SELECT city FROM city WHERE city_id = %s;"
    cursor.execute(query, (cityID,))
    oldCity = cursor.fetchall()[0][0]
    newCityID = ""
    if (oldCity == city):
        newCityID = cityID
    else: 
        # Check if city already exists and if so store it in variable. Else, add it and store ID.
        query = "SELECT city_id FROM city WHERE city = %s;"
        cursor.execute(query, (city,))
        cityExists = cursor.fetchall()
        if (len(cityExists) != 0):
            newCityID = cityExists[0][0]
        else: 
            query = "INSERT INTO city(city, country_id, last_update) VALUES (%s, %s NOW())"
            cursor.execute(query, (city, newCountryID,))
            query = "SELECT city_id FROM city WHERE city = %s;"
            cursor.execute(query, (city,))
            newCityID = cursor.fetchall()[0][0]

    # Check if any aspect of address changed. If so, need new address ID.
    query = "SELECT address, address2, district, postal_code, phone FROM address WHERE address_id = %s;"
    cursor.execute(query, (addressID,))
    oldInfo = cursor.fetchall()
    oldAddress = oldInfo[0][0]
    oldAddress2 = oldInfo[0][1]
    oldDistrict = oldInfo[0][2]
    oldPostalCode = oldInfo[0][3]
    oldPhone = oldInfo[0][4]

    if (oldAddress != address or oldAddress2 != address2 or oldDistrict != district or oldPostalCode != postal_code or oldPhone != phone):
        # Update Address ID to have new Address Info
        query = "UPDATE address SET address = %s, address2 = %s, district = %s, postal_code = %s, phone = %s, city_id = %s WHERE address_id = %s"
        cursor.execute(query, (address, address2, district, postal_code, phone, newCityID, addressID,))
    
    # Update Existing Customer
    query = "UPDATE customer SET first_name = %s, last_name = %s, email = %s WHERE customer_id = %s;"
    cursor.execute(query, (firstName, lastName, email, custID,))
    conn.commit()

    cursor.close()
    return 'Done', 200

# Delete Customer
@app.route("/deletecustomer", methods=['DELETE'])
def deletecustomer():
    conn = mysql.connection
    data = request.get_json()
    custID = data[0]['customer_id']

    # Check for current rentals
    cursor = conn.cursor()
    query = """SELECT customer.customer_id, COUNT(*) as count FROM customer
	            JOIN rental ON customer.customer_id = rental.customer_id
	            WHERE return_date IS NULL AND customer.customer_id = {}
	            GROUP BY customer.customer_id ORDER BY COUNT(*) DESC""".format(custID)
    cursor.execute(query)
    rentals = cursor.fetchall()
    if (len(rentals)) > 0:
        response = make_response("Error, Customer currently has rentals.")
        response.headers["error"] = "Currently Renting!"
        response.status_code = 400
        return response
    
    # Make sure customer has no outstanding balance
    query = "SELECT get_customer_balance({}, NOW())".format(custID)
    cursor.execute(query)
    balance = cursor.fetchall()
    if balance[0][0] > 0.00:
        response = make_response("Error, Outstanding Balance")
        response.headers["error"] = "Outstanding Balance"
        response.status_code = 400
        return response
    
    # Check for payment history
    query = "SELECT * FROM payment WHERE customer_id = {};".format(custID)
    cursor.execute(query)
    paymentHistory = cursor.fetchall()

    # If so, delete all payment history and reset autoincrementer.
    if len(paymentHistory) > 0:
        query = "DELETE FROM payment WHERE customer_id = {};".format(custID)
        cursor.execute(query)
        query = "ALTER TABLE payment AUTO_INCREMENT = 1;"
        cursor.execute(query)
    
    # Check for rental history
    query = "SELECT * FROM rental WHERE customer_id = {};".format(custID)
    cursor.execute(query)
    rentalHistory = cursor.fetchall()

    # If so, delete all rental history and reset autoincrementer.
    if len(rentalHistory) > 0:
        query = "DELETE FROM rental WHERE customer_id = {};".format(custID)
        cursor.execute(query)
        query = "ALTER TABLE rental AUTO_INCREMENT = 1;"
        cursor.execute(query)
    
    # Finally, delete the customer and reset the autoincrementer.
    query = "DELETE FROM customer WHERE customer_id = {};".format(custID)
    cursor.execute(query)
    query = "ALTER TABLE customer AUTO_INCREMENT = 1;"
    cursor.execute(query)
    conn.commit()

    cursor.close()
    return 'Done', 200

if __name__ == "__main__":
    app.run(debug=True)