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
    query = """SELECT * from customer
                ORDER BY customer_id"""
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
    query = "SELECT * from customer WHERE customer_id = {}".format(id)
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

    # Make sure inputs are not invalid
    if (firstName == "" or lastName == "" or email == ""):
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

    # Add New Customer
    query = """INSERT INTO customer(store_id, first_name, last_name, email, address_id, active, create_date, last_update)
                         VALUES(1, %s, %s, %s, 1, 1, NOW(), NOW())"""
    cursor.execute(query, (firstName, lastName, email,))
    conn.commit()

    cursor.close()
    return 'Done', 200

# Edit Existing Customer
@app.route("/editcustomer", methods=['POST'])
def editcustomer():
    conn = mysql.connection
    data = request.get_json()
    firstName = data[0]['first_name']
    lastName = data[1]['last_name']
    email = data[2]['email']
    custID = data[3]['customer_id']

    # Make sure inputs are not invalid
    if (firstName == "" or lastName == "" or email == ""):
        response = make_response("Error, Fields Invalid")
        response.headers["error"] = "Invalid Fields"
        response.status_code = 400
        return response
    
    # Update Existing Customer
    cursor = conn.cursor()
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