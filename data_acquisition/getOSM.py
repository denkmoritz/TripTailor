import psycopg2
import subprocess
import os

# Connection details
dbname = "gis"  # Your database to connect to
user = "localhost"    # Your username
password = "****"    # Your password
host = "localhost"   # Host where the database is running
port = "5432"        # Change port if necessary

new_DB_name = "osm_data"
osm_file = "./muenster-regbez-latest.osm.pbf"
flex_config_file = "./generic.lua"

# Connect to the default 'postgres' database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

conn.autocommit = True  # Disable transaction block for this connection
cur = conn.cursor()

# Check if the new database already exists
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{new_DB_name}';")
db_exists = cur.fetchone()

if db_exists:
    print(f"Database '{new_DB_name}' already exists. Skipping creation.")
else:
    # Create a new database
    cur.execute(f"CREATE DATABASE {new_DB_name};")
    print(f"Database '{new_DB_name}' created successfully.")

# Connect to the newly created database
conn = psycopg2.connect(
    dbname=new_DB_name,
    user=user,
    password=password,
    host=host,
    port=port
)

conn.autocommit = True  # Ensure auto-commit is enabled
cur = conn.cursor()

# Enable the PostGIS and hstore extensions
try:
    cur.execute("CREATE EXTENSION postgis;")
    print("PostGIS extension enabled successfully.")
    cur.execute("CREATE EXTENSION hstore;")
    print("Hstore extension enabled successfully.")
except psycopg2.Error as e:
    print(f"Error enabling extensions: {e}")

# Clean up and close the connection
cur.close()
conn.close()

# Set the PGPASSWORD environment variable to avoid password prompt for osm2pgsql
os.environ["PGPASSWORD"] = password

# Construct the osm2pgsql command
command = [
    "osm2pgsql",
    "--output=flex",        # Use flex output mode
    "--style", flex_config_file,  # Specify the Lua configuration file for flex mode
    "-d", new_DB_name,      # PostgreSQL database name
    "-U", user,             # PostgreSQL username
    "-H", host,             # Host (default is localhost)
    "-P", port,             # Specify the PostgreSQL port here
    "--create",             # Mode (create or append)
    "--slim",               # Use slim tables to reduce memory usage
    "-G",                   # Add geometry types
    "-k",                   # Use flat nodes
    "--hstore",             # Include tags in an hstore column
    osm_file                # Input OSM file
]

# Run osm2pgsql to import data
try:
    process = subprocess.run(command, check=True, text=True)
    print("osm2pgsql executed successfully with flex output.")
except subprocess.CalledProcessError as e:
    print(f"Error running osm2pgsql: {e}")

# Verify data using psycopg2
try:
    conn = psycopg2.connect(
        dbname=new_DB_name,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()

    # Example query to count imported rows in a table
    cursor.execute("SELECT COUNT(*) FROM planet_osm_polygon;")
    count = cursor.fetchone()[0]
    print(f"Total polygons imported: {count}")

    cursor.close()
    conn.close()
except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")