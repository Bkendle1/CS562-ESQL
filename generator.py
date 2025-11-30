import subprocess
import sys

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    # Receive Phi operands (assumes that all lists are comma-separated)
    # stores phi operands
    phi = {
        "S": [], # list of all projected attributes and aggregates in SELECT
        "n": 0, # number of grouping variables
        "V": [], # list of grouping attributes before 
        "F": [], # aggregates for each group (both grouping attributes' and grouping variables')
        "sigma": [], # predicates for each grouping variable specified in SUCH THAT
        "G": "" # predicate in HAVING 
    } 

    # If no filepath was given in the command line, then we assume the user will input operands manually
    if len(sys.argv) == 1:
        print("No file given")
        # ask user for phi operands
        S = input("List all select attributes: ")
        n = input("Number of grouping variables: ")
        V = input("List all grouping attributes: ")
        F = input("List all aggregates: ")
        sigma = input("List grouping variable predicates: ")
        G = input("List predicates for output of GROUP BY: ")
    elif len(sys.argv) == 2:
        # read phi operands from file that the user provided
        with open(f"{sys.argv[1]}", "r") as file:
            S = file.readline().strip()
            S = S.split(": ")[-1]

            n = file.readline().strip()
            n = n.split(": ")[-1]
            n = int(n)

            V = file.readline().strip()
            V = V.split(": ")[-1]

            F = file.readline().strip()
            F = F.split(": ")[-1]
            
            sigma = file.readline().strip()
            sigma = sigma.split(": ")[-1]

            G = file.readline().strip()
            G = G.split(": ")[-1]
    else:
        print("Error: Too many arguments")
        exit()
    
    # print(f"S: {S}\nn: {n}\nV: {V}\nF: {F}\nsigma: {sigma}\nG: {G}")
    
    phi["S"] = S.split(", ")
    phi["n"] = n
    phi["V"] = V.split(", ")
    phi["F"] = [[]] * (n + 1) # create an empty list to store aggregates for each of the n grouping variables +1 list for the aggregates for the standard SQL groups which would be at index 0
    F = F.split(", ")
    # iterate through each aggregate and add them to the list associated with the matching grouping variable
    for agg in F:
        idx = int(agg[0]) # compute what grouping variable the current aggregate is for (idx = 0 are the standard SQL groups)
        phi["F"][idx] = phi["F"][idx] + [agg]
    phi["sigma"] = sigma.split(", ") # initialize with an empty list for each grouping variable
    phi["G"] = G 
    
    # print(f"S: {S}\nn: {n}\nV: {V}\nF: {F}\nsigma: {sigma}\nG: {G}")

    # TODO generate the code to create the H table
    # mf_struct must have a column for each grouping attribute and aggregate
    mf_struct = """
    mf_struct = {
    """
    # populate mf_struct with grouping attributes
    for attr in phi.get("V"):
        mf_struct += f"\t'{attr}' : [],\n"
    # populate mf_struct with aggregate functions
    for agg in F:
        if agg != []:
            mf_struct += f"\t\t'{agg}': [],\n"
    mf_struct += "\t}" # close mf_struct
    
    mf_struct = """mf_struct = {
    	'cust' : ["Sam", "Dan", "Adora", "Catra"],
		'1_sum_quant': [96, 68, 26, 86],
		'1_avg_quant': [22, 57, 50, 99],
		'2_sum_quant': [10, 1, 94, 33],
		'3_sum_quant': [55, 54, 78, 56],
		'3_avg_quant': [10, 34, 56, 76],
	}
    """
    # print the mf_struct
    debug = f"""
    print(tabulate.tabulate(mf_struct, headers="keys", tablefmt="psql")) # DEBUG
    """

    # TODO generate the code that implements the evaluation algorithm

    # peform n + 1 scans
    # scan 0 adds rows with distinct grouping attributes as well as computes any aggregates for the groups defined by the grouping attributes
    lookup = """
def lookup(cur: dict, struct: dict, size: int, attrs: list) -> int: 
    \"""
    Search for a given "group by" attribute value(s) in mf_struct. 
    If the value(s) doesn't exist then return -1, else return the index for that row.

    :param cur: Current row in the mf_struct
    :param struct: The mf_struct
    :param size: Number of rows in the mf_struct
    :return: Either the index of the matching row in mf_struct or -1 if not found.
    \"""
    # iterate through each row in mf_struct
    for i in range(size):
        # iterate through each grouping attribute
        for attr in attrs:
            # print(f"Cur: {cur[attr]} | MF-Struct: {struct.get(attr)[i]}")
            # mf_struct has the attribute value 
            if struct.get(attr)[i] == cur[attr]:
                continue # check next attribute
            return -1 # mf_struct doesn't have the grouping attribute values in the given row 
    return i # mf_struct has all grouping attribute values in the given row
"""
    
    # body = """
    # # for idx, (key, val) in enumerate(mf_struct.items()):
    # #     print(f"Index: {idx} | Key: {key} | Value: {val}")
    # """
    
    body = """
    for row in cur:
        lookup(row, mf_struct, len(mf_struct["cust"]), ["cust"])
    """

    # body = """
    # for row in cur:
    #     if row['quant'] > 10:
    #         _global.append(row)
    # """
    


    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

# Helper functions
{lookup}

def query():
    load_dotenv() # reads the .env file

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales") # prints all the rows in the data table
    
    _global = []
    {mf_struct}
    num_rows = 1 # keeps track of how many rows the mf_struct has
    {body}
    
    {debug}
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql") # returns data as a table

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """
    
    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # COMMENTED OUT FOR TESTING PURPOSES
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
