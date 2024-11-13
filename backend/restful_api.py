import asyncio
import json
import psycopg2
from websockets import connect
from flask import Flask, jsonify, request

# Initialize Flask app for RESTful API
app = Flask(__name__)

# Database connection setup
conn = psycopg2.connect(
    dbname='database',
    user='postgres',
    password='@En0806833670',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS machine_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    power FLOAT,
    voltage_l1_gnd FLOAT,
    voltage_l2_gnd FLOAT,
    voltage_l3_gnd FLOAT,
    pressure FLOAT,
    force FLOAT,
    cycle_count INT,
    position_of_punch FLOAT
)
''')
conn.commit()

async def handle_websocket():
    websocket_url = "ws://technest.ddns.net:8001/ws"
    try:
        async with connect(websocket_url) as websocket:
            await websocket.send("6807e4259a8a69f3f352cfc2fae2117f")
            print("WebSocket connection opened.")

            while True:
                message = await websocket.recv()
                
                try:
                    data = json.loads(message)
                    power = data["Energy Consumption"].get("Power")
                    voltage_l1_gnd = data["Voltage"].get("L1-GND")
                    voltage_l2_gnd = data["Voltage"].get("L2-GND")
                    voltage_l3_gnd = data["Voltage"].get("L3-GND")
                    pressure = data.get("Pressure")
                    force = data.get("Force")
                    cycle_count = data.get("Cycle Count")
                    position_of_punch = data.get("Position of the Punch")

                    # Insert data into the PostgreSQL database.
                    try:
                        cursor.execute(
                            """INSERT INTO machine_data 
                            (power, voltage_l1_gnd, voltage_l2_gnd, voltage_l3_gnd, pressure, force, cycle_count, position_of_punch) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (power, voltage_l1_gnd, voltage_l2_gnd, voltage_l3_gnd, pressure, force, cycle_count, position_of_punch)
                        )
                        conn.commit()
                        # print("Data saved to database.")
                    except psycopg2.Error as db_error:
                        conn.rollback()
                        print(f"Database error: {db_error}")

                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error processing message: {e}")

    except Exception as e:
        print(f"WebSocket connection error: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")

# Flask route to get all machine data
@app.route('/data', methods=['GET'])
def get_all_data():
    cursor.execute("SELECT * FROM machine_data ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    data = [
        {
            "id": row[0],
            "timestamp": row[1],
            "power": row[2],
            "voltage_l1_gnd": row[3],
            "voltage_l2_gnd": row[4],
            "voltage_l3_gnd": row[5],
            "pressure": row[6],
            "force": row[7],
            "cycle_count": row[8],
            "position_of_punch": row[9]
        }
        for row in rows
    ]
    return jsonify(data), 200

# Flask route to get machine data by ID
@app.route('/data/<int:data_id>', methods=['GET'])
def get_data_by_id(data_id):
    cursor.execute("SELECT * FROM machine_data WHERE id = %s", (data_id,))
    row = cursor.fetchone()
    if row:
        data = {
            "id": row[0],
            "timestamp": row[1],
            "power": row[2],
            "voltage_l1_gnd": row[3],
            "voltage_l2_gnd": row[4],
            "voltage_l3_gnd": row[5],
            "pressure": row[6],
            "force": row[7],
            "cycle_count": row[8],
            "position_of_punch": row[9]
        }
        return jsonify(data), 200
    else:
        return jsonify({"error": "Data not found"}), 404

# POST: Create new machine data
@app.route('/data', methods=['POST'])
def create_data():
    new_data = request.get_json()
    try:
        cursor.execute(
            """INSERT INTO machine_data 
            (power, voltage_l1_gnd, voltage_l2_gnd, voltage_l3_gnd, pressure, force, cycle_count, position_of_punch) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (new_data["power"], new_data["voltage_l1_gnd"], new_data["voltage_l2_gnd"], 
             new_data["voltage_l3_gnd"], new_data["pressure"], new_data["force"], 
             new_data["cycle_count"], new_data["position_of_punch"])
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        return jsonify({"message": "Data created", "id": new_id}), 201
    except psycopg2.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

# PUT: Update machine data by ID
@app.route('/data/<int:data_id>', methods=['PUT'])
def update_data(data_id):
    update_data = request.get_json()
    try:
        cursor.execute(
            """UPDATE machine_data SET 
            power = %s, voltage_l1_gnd = %s, voltage_l2_gnd = %s, 
            voltage_l3_gnd = %s, pressure = %s, force = %s, 
            cycle_count = %s, position_of_punch = %s 
            WHERE id = %s""",
            (update_data["power"], update_data["voltage_l1_gnd"], update_data["voltage_l2_gnd"], 
             update_data["voltage_l3_gnd"], update_data["pressure"], update_data["force"], 
             update_data["cycle_count"], update_data["position_of_punch"], data_id)
        )
        if cursor.rowcount == 0:
            return jsonify({"error": "Data not found"}), 404
        conn.commit()
        return jsonify({"message": "Data updated"}), 200
    except psycopg2.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

# DELETE: Delete machine data by ID
@app.route('/data/<int:data_id>', methods=['DELETE'])
def delete_data(data_id):
    try:
        cursor.execute("DELETE FROM machine_data WHERE id = %s", (data_id,))
        if cursor.rowcount == 0:
            return jsonify({"error": "Data not found"}), 404
        conn.commit()
        return jsonify({"message": "Data deleted"}), 200
    except psycopg2.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

# Run Flask and WebSocket in parallel
async def main():
    websocket_task = asyncio.create_task(handle_websocket())
    
    # Run Flask app in a separate thread
    from threading import Thread
    def run_flask():
        app.run(host="0.0.0.0", port=5000)

    flask_thread = Thread(target=run_flask)
    flask_thread.start()
    
    await websocket_task

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Program interrupted. Exiting gracefully.")