#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE

    myHost = "awsprddbs4836.shared.sydney.edu.au"
    userid = "y25s1c9120_hcao0487"
    passwd = "eGJ4Ek3g"
    
    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate salesperson based on username and password
'''
def checkLogin(login, password):
    conn = None
    cur  = None

    try:
        conn = openConnection()
        cur  = conn.cursor()

        sql = """
        select username, firstname as "firstName", lastname as "lastName"
        from Salesperson
        where username ilike %s and password = %s
        """
        
        cur.execute(sql, (login, password))
        return cur.fetchone()   
        
    except:
        raise
    finally:
        if cur:  cur.close()
        if conn: conn.close()


"""
    Retrieves the summary of car sales.

    This method fetches the summary of car sales from the database and returns it 
    as a collection of summary objects. Each summary contains key information 
    about a particular car sale.

    :return: A list of car sale summaries.
"""
def getCarSalesSummary():
    conn = None
    cur = None
    result = []
    
    try:
        conn = openConnection()
        cur = conn.cursor()
        
        sql = """
        select m.makename as make, md.modelname as model,
        count(case when cs.issold = false then 1 end) as availableUnits,
        count(case when cs.issold = true then 1 end) as soldUnits,
        coalesce(sum(case when cs.issold = true then cs.price else 0 end), 0) as soldTotalPrices,
        max(case when cs.issold = true then cs.saledate else null end) as lastPurchaseAt
        from Model md
        join Make m on md.makecode = m.makecode
        left join CarSales cs on md.modelcode = cs.modelcode and md.makecode = cs.makecode
        group by m.makename, md.modelname
        order by m.makename, md.modelname
        """
        
        cur.execute(sql)
        rows = cur.fetchall()
        
        for row in rows:
            price = "{:.2f}".format(float(row[4])) if row[4] else "0.00"
            
            date_str = ''
            if row[5]:
                date_obj = row[5]
                date_str = date_obj.strftime("%d-%m-%Y")
            
            summary = {
                'make': row[0],
                'model': row[1],
                'availableUnits': row[2] or 0,
                'soldUnits': row[3] or 0,
                'soldTotalPrices': price,
                'lastPurchaseAt': date_str
            }
            result.append(summary)
            
        return result
        
    except Exception as e:
        print(f"Error retrieving car sales summary: {str(e)}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()

"""
    Finds car sales based on the provided search string.

    This method searches the database for car sales that match the provided search 
    string. See assignment description for search specification

    :param search_string: The search string to use for finding car sales in the database.
    :return: A list of car sales matching the search string.
"""
def findCarSales(searchString):
    conn = None
    cur = None
    result = []
    
    try:
        conn = openConnection()
        cur = conn.cursor()
        
        sql = """
        select cs.carsaleid, m.makename, md.modelname, cs.builtyear, cs.odometer, cs.price, 
        cs.issold, cs.saledate, c.firstname, c.lastname, s.firstname, s.lastname
        from CarSales cs
        join Make m on cs.makecode = m.makecode
        join Model md on cs.modelcode = md.modelcode
        left join Customer c on cs.buyerid = c.customerid
        left join Salesperson s on cs.salespersonid = s.username

        where (m.makename ilike %s or md.modelname ilike %s or cast(cs.builtyear as text) ilike %s
        or (c.firstname ilike %s or c.lastname ilike %s) or (s.firstname ilike %s or s.lastname ilike %s)
        or %s = '')
        and (cs.issold = false or (cs.issold = true and cs.saledate >= current_date - interval '3 years'))
        
        order by cs.issold, case when cs.issold = true then cs.saledate end asc,
        m.makename asc, md.modelname asc
        """
        
        search_pattern = f"%{searchString}%"
        
        cur.execute(sql, (search_pattern, search_pattern, search_pattern, 
                         search_pattern, search_pattern, search_pattern, 
                         search_pattern, searchString))
        rows = cur.fetchall()
        
        for row in rows:
            date_str = ''
            if row[7]:
                date_obj = row[7]
                date_str = date_obj.strftime("%d-%m-%Y")
                
            customer = f"{row[8]} {row[9]}" if row[8] and row[9] else ""
            
            salesperson = f"{row[10]} {row[11]}" if row[10] and row[11] else ""
            
            issold_str = "True" if row[6] else "False"
            
            price_str = "{:.2f}".format(float(row[5])) if row[5] else "0.00"
            
            car_sale = {
                'ID': row[0], 'id': row[0],
                'make': row[1],
                'model': row[2],
                'Year': row[3], 'year': row[3],
                'odometer': row[4],
                'price': price_str,
                'issold': issold_str,
                'saledate': date_str,
                'buyer': customer,
                'salesperson': salesperson
            }
            result.append(car_sale)
            
        return result
        
    except Exception as e:
        print(f"Error searching car sales: {str(e)}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()

"""
    Adds a new car sale to the database.

    This method accepts a CarSale object, which contains all the necessary details 
    for a new car sale. It inserts the data into the database and returns a confirmation 
    of the operation.

    :param car_sale: The CarSale object to be added to the database.
    :return: A boolean indicating if the operation was successful or not.
"""
def addCarSale(make, model, builtYear, odometer, price):
    return

"""
    Updates an existing car sale in the database.

    This method updates the details of a specific car sale in the database, ensuring
    that all fields of the CarSale object are modified correctly. It assumes that 
    the car sale to be updated already exists.

    :param car_sale: The CarSale object containing updated details for the car sale.
    :return: A boolean indicating whether the update was successful or not.
"""
def updateCarSale(carsaleid, customer, salesperosn, saledate):
    return
