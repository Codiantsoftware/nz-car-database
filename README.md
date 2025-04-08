# Vehicle Information Database Setup

This project involves setting up a PostgreSQL database to store vehicle information and importing data from a JSON file. Follow the instructions below to configure your environment, create the necessary database table, and import the data.

## Prerequisites

- PostgreSQL installed and running.
- Node.js installed.
- npm (Node Package Manager) installed.

## Steps to Set Up the Database

1. **Create a PostgreSQL Database**

   ```sql
   CREATE DATABASE "car-info";
   ```

2. **Install Required Node.js Packages**

   ```bash
   npm install
   ```

3. **Configure Database Connection**

   Ensure the database connection settings in the script match your PostgreSQL setup:

   ```javascript
   const pool = new Pool({
     host: "localhost",
     port: 5432,
     user: "postgres",
     password: "",
     database: "car-info",
   });
   ```

4. **Prepare Your JSON Data File**

   Make sure your vehicle data is in a JSON file named `car_data.json` in the same directory as the script, or update the file path in the script.

5. **Run the Data Import Script**
   ```bash
   node import_vehicle.js
   ```

## How the Script Works

The script performs the following operations:

1. Creates a `vehicle_info` table in the PostgreSQL database if it doesn't already exist.
2. Reads the JSON file as a stream to efficiently handle large files.
3. Processes records in batches (default batch size: 1000 records).
4. Maps JSON fields to database columns according to a predefined schema.
5. Inserts data into the database using transactions for data integrity.
6. Logs progress and errors during the import process.

## Database Schema

The `vehicle_info` table includes fields for:

- Vehicle identification (VIN, plate numbers, chassis)
- Vehicle specifications (make, model, year, type)
- Registration information
- Technical details (engine size, transmission, fuel type)
- Environmental metrics (CO2 emissions, fuel consumption)
- And more

## Error Handling

The script includes robust error handling with:

- Transaction rollback on batch insertion failure
- Detailed error logging
- Graceful cleanup of database resources

## Customization

You can customize the script by modifying:

- `BATCH_SIZE` constant: Adjust the number of records processed in each batch
- Database connection parameters
- Field mapping in the `insertBatch` function to match your JSON structure

## Troubleshooting

If you encounter issues:

1. Verify PostgreSQL is running and accessible
2. Check that the database credentials are correct
3. Ensure your JSON data structure matches the expected format
4. Review console output for specific error messages
