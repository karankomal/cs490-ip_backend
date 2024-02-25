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

@app.route("/returnfilm", methods=['POST'])
def returnfilm():
    conn = mysql.connection

@app.route("/editcustomer", methods=['POST'])
def editcustomer():
    conn = mysql.connection

@app.route("/deletecustomer", methods=['POST'])
def deletecustomer():
    conn = mysql.connection


if __name__ == "__main__":
    app.run(debug=True)