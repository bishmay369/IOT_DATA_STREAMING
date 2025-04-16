# 🔧 Real-Time IoT Data Pipeline with Microsoft Fabric

This project demonstrates a **real-time data ingestion and transformation pipeline** built using **Microsoft Fabric**. The pipeline simulates IoT sensor data using a Python script, processes it using **KQL (Kusto Query Language)** in an Event Stream, and stores the final output in a **Lakehouse**.

---

## 📌 Project Workflow

### 1. 🐍 IoT Data Simulation with Python
- A Python script (`iot_data_generator.py`) is used to simulate real-time IoT sensor data (e.g., temperature, humidity).
- The data is sent to an Eventstream input using REST API or direct connector.

### 2. ⚡ Microsoft Fabric Event Stream
- Configured an **Eventstream** in Microsoft Fabric to receive and route the streaming data.
- Connected the Eventstream to a **KQL Database (Real-Time Analytics)** as a destination.

### 3. 🔍 KQL Database Transformation
- Inside the KQL database, used **KQL queries** to filter, transform, and enrich the incoming IoT data.
- Applied logic such as value filtering, anomaly tagging, or unit conversion.

### 4. 🗃️ Output to Lakehouse
- Final transformed data is written to a **Lakehouse table** for storage and further analytics or reporting in Power BI.

---

## 📁 Project Structure

```bash
.
├── iot_data_generator.py        # Python script to simulate and send IoT data
├── kql_transformations.kql      # All KQL queries used for processing
├── eventstream_config.md        # Setup steps for Eventstream in Fabric
├── lakehouse_output_structure.md # Description of output Lakehouse schema
├── README.md                    # Project overview
