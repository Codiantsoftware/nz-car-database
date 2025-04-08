const fs = require('fs');
const JSONStream = require('JSONStream');
const { Pool } = require('pg');
const BATCH_SIZE = 1000;

// Database connection
const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: '123',
  database: 'car-info',
});

/**
 * Process a large JSON file in batches
 */
async function processLargeJSONFile() {
  const stream = fs.createReadStream('car_data.json', { encoding: 'utf8' });
  const jsonStream = JSONStream.parse('*'); // Assuming the JSON file has an array as the root element
  const dataBatch = [];

  stream.pipe(jsonStream);

  // Pause the stream to control flow
  jsonStream.on('data', async (data) => {
    jsonStream.pause();
    dataBatch.push(data);

    if (dataBatch.length >= BATCH_SIZE) {
      try {
        await insertBatch(dataBatch);
        console.log(`Processed batch of ${dataBatch.length} records`);
        dataBatch.length = 0; // Clear the batch
      } catch (error) {
        console.error("Error processing batch:", error);
      }
    }
    jsonStream.resume();
  });

  jsonStream.on('end', async () => {
    if (dataBatch.length > 0) {
      try {
        await insertBatch(dataBatch); // Insert any remaining items
        console.log(`Processed final batch of ${dataBatch.length} records`);
      } catch (error) {
        console.error("Error processing final batch:", error);
      }
    }
    console.log("All data inserted successfully.");
    await pool.end();
  });

  jsonStream.on('error', (error) => {
    console.error("Error reading JSON file:", error);
    pool.end();
  });
}

/**
 * Insert a batch of records into PostgreSQL
 * @param {Array} data - Array of objects from the JSON file
 */
const insertBatch = async (data) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const insertPromises = data.map(item => {
      // Map JSON fields to database columns
      const values = [
        item.vin11 || null,                                // VIN
        item.vehicleYear || null,                          // Year
        item.make || null,                                 // Make
        item.model || null,                                // Model
        item.submodel || null,                             // Submodel
        item.vehicleType || item.bodyType || null,         // Type
        null,                                              // Odometer (not in JSON)
        item.basicColour || null,                          // Colour
        item.motivePower || null,                          // Fuel
        item.transmissionType || null,                     // Transmission
        null,                                              // Drive (not in JSON)
        formatNZDate(item.firstNzRegistrationYear, item.firstNzRegistrationMonth) || null, // FirstNZRegistration
        item.originalCountry || null,                      // OriginalCountry
        item.previousCountry || null,                      // PreviousCountry
        null,                                              // NumberOfOwners (not in JSON)
        null,                                              // RRPVehicle (not in JSON)
        null,                                              // RRP (not in JSON)
        null,                                              // ResidualValue (not in JSON)
        null,                                              // LowValue (not in JSON)
        null,                                              // AverageValue (not in JSON)
        null,                                              // HighValue (not in JSON)
        null,                                              // Confidence (not in JSON)
        null,                                              // Plate (duplicate column, not in JSON)
        null,                                              // VIN (duplicate column, not in JSON)
        item.industryModelCode || item.mvmaModelCode || null, // ModelCode
        item['﻿objectid'] || null,                         // VehicleID
        null,                                              // ErrorCode (not in JSON)
        null,                                              // Explanation (not in JSON)
        null,                                              // LabelVersion (not in JSON)
        null,                                              // LabelID (not in JSON)
        null,                                              // LastRegUpdate (not in JSON)
        item.mvmaModelCode || item.industryModelCode || null, // ModelCode (duplicate column)
        null,                                              // Variant (not in JSON)
        item.make || null,                                 // Make (duplicate column)
        item.model || null,                                // Model (duplicate column)
        item.submodel || null,                             // SubModel (duplicate column)
        item.vehicleType || item.bodyType || null,         // VehicleType (duplicate column)
        item.numberOfSeats || null,                        // Seats
        null,                                              // Doors (not in JSON)
        null,                                              // LabelFormat (not in JSON)
        item.motivePower || item.alternativeMotivePower || null, // FuelType
        item.ccRating || null,                             // EngineSize
        item.powerRating || null,                          // EnginePower
        item.transmissionType || null,                     // Transmission (duplicate column)
        null,                                              // Drive (duplicate column, not in JSON)
        item.vdamWeight || item.grossVehicleMass || null,  // Weight
        null,                                              // FuelStars (not in JSON)
        item.fcCombined || null,                           // FuelConsumption
        null,                                              // YearlyCost (not in JSON)
        null,                                              // RUCrate (not in JSON)
        null,                                              // RUC (not in JSON)
        null,                                              // CO2Stars (not in JSON)
        item.syntheticGreenhouseGas || null,               // CO2
        null,                                              // YearlyCO2 (not in JSON)
        null,                                              // SafetyStars (not in JSON)
        null,                                              // DriverSafetyStars (not in JSON)
        null,                                              // DriverSafetyTest (not in JSON)
        null,                                              // FuelPromoBadge (not in JSON)
        null,                                              // SafetyPromoBadge (not in JSON)
        null,                                              // LikeVehicle (not in JSON)
        null,                                              // Item (not in JSON)
        item.chassis7 || null,                             // Chassis
        item.vehicleYear || null,                          // mvrYear (duplicate)
        formatNZDate(item.firstNzRegistrationYear, item.firstNzRegistrationMonth) || null, // mvrFirstNZreg
        item.originalCountry || null,                      // mvrOrigCntry (duplicate)
        item.previousCountry || null,                      // mvrPrevCntry (duplicate)
        item.importStatus || null,                         // mvrRegType
        null,                                              // mvrRegExpires (not in JSON)
        null,                                              // mvrWOFExpires (not in JSON)
        null,                                              // mvrODO (not in JSON)
        null,                                              // mvrTotOwners (not in JSON)
        item.basicColour || null,                          // mvrColourBase (duplicate)
        null,                                              // mvrColourSecondary (not in JSON)
        item.make || null,                                 // mvrRegMake (duplicate)
        item.model || null,                                // mvrRegModel (duplicate)
        item.submodel || null,                             // mvrRegSubModel (duplicate)
        null,                                              // SubmodelSpec (not in JSON)
        new Date().toISOString(),                          // CreatedDateUTC
        new Date().toISOString(),                          // LastUpdatedUTC
        false                                              // IsCached
      ];

      // Create the SQL query with placeholders
      const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

      return client.query(
        `INSERT INTO vehicle_info (
          plate, year, make, model, submodel, type, odometer, colour, fuel, transmission, 
          drive, firstnzregistration, originalcountry, previouscountry, numberofowners, 
          rrpvehicle, rrp, residualvalue, lowvalue, averagevalue, highvalue, confidence, 
          plate2, vin, modelcode, vehicleid, errorcode, explanation, labelversion, labelid, 
          lastregupdate, modelcode2, variant, make2, model2, submodel2, vehicletype, seats, 
          doors, labelformat, fueltype, enginesize, enginepower, transmission2, drive2, 
          weight, fuelstars, fuelconsumption, yearlycost, rucrate, ruc, co2stars, co2, 
          yearlyco2, safetystars, driversafetystars, driversafetytest, fuelpromobadge, 
          safetypromobadge, likevehicle, item, chassis, mvryear, mvrfirstnzreg, mvrorigcntry, 
          mvrprevcntry, mvrregtype, mvrregexpires, mvrwofexpires, mvrodo, mvrtotowners, 
          mvrcolourbase, mvrcoloursecondary, mvrregmake, mvrregmodel, mvrregsubmodel, 
          submodelspec, createddateutc, lastupdatedutc, iscached
        ) VALUES (${placeholders})`,
        values
      );
    });

    await Promise.all(insertPromises);
    await client.query('COMMIT');
  } catch (error) {
    console.error("Error inserting batch:", error.message);
    console.error("Stack trace:", error.stack);
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
};

/**
 * Format NZ registration date from year and month
 * @param {number} year 
 * @param {number} month 
 * @returns {string|null} Formatted date string or null
 */
function formatNZDate(year, month) {
  if (!year) return null;

  const formattedMonth = month ? String(month).padStart(2, '0') : '01';
  return `${year}-${formattedMonth}-01`;
}


/**
 * Create the vehicle_info table if it does not already exist in the database.
 * The table is created with all the columns that are present in the JSON data.
 * This function is idempotent, meaning it can be called multiple times without
 * causing any issues.
 * @throws {Error} If there is a problem creating the table
 */
async function createTableIfNotExists() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS vehicle_info (
        id SERIAL PRIMARY KEY,
        plate VARCHAR(255),
        year INTEGER,
        make VARCHAR(255),
        model VARCHAR(255),
        submodel VARCHAR(255),
        type VARCHAR(255),
        odometer INTEGER,
        colour VARCHAR(255),
        fuel VARCHAR(255),
        transmission VARCHAR(255),
        drive VARCHAR(255),
        firstnzregistration DATE,
        originalcountry VARCHAR(255),
        previouscountry VARCHAR(255),
        numberofowners INTEGER,
        rrpvehicle BOOLEAN,
        rrp DECIMAL(12,2),
        residualvalue DECIMAL(12,2),
        lowvalue DECIMAL(12,2),
        averagevalue DECIMAL(12,2),
        highvalue DECIMAL(12,2),
        confidence DECIMAL(5,2),
        plate2 VARCHAR(255),
        vin VARCHAR(255),
        modelcode VARCHAR(255),
        vehicleid VARCHAR(255),
        errorcode VARCHAR(255),
        explanation TEXT,
        labelversion VARCHAR(255),
        labelid VARCHAR(255),
        lastregupdate TIMESTAMP,
        modelcode2 VARCHAR(255),
        variant VARCHAR(255),
        make2 VARCHAR(255),
        model2 VARCHAR(255),
        submodel2 VARCHAR(255),
        vehicletype VARCHAR(255),
        seats INTEGER,
        doors INTEGER,
        labelformat VARCHAR(255),
        fueltype VARCHAR(255),
        enginesize INTEGER,
        enginepower INTEGER,
        transmission2 VARCHAR(255),
        drive2 VARCHAR(255),
        weight INTEGER,
        fuelstars INTEGER,
        fuelconsumption DECIMAL(8,2),
        yearlycost DECIMAL(12,2),
        rucrate DECIMAL(8,2),
        ruc BOOLEAN,
        co2stars INTEGER,
        co2 VARCHAR(255),
        yearlyco2 DECIMAL(8,2),
        safetystars INTEGER,
        driversafetystars INTEGER,
        driversafetytest VARCHAR(255),
        fuelpromobadge VARCHAR(255),
        safetypromobadge VARCHAR(255),
        likevehicle BOOLEAN,
        item VARCHAR(255),
        chassis VARCHAR(255),
        mvryear INTEGER,
        mvrfirstnzreg DATE,
        mvrorigcntry VARCHAR(255),
        mvrprevcntry VARCHAR(255),
        mvrregtype VARCHAR(255),
        mvrregexpires DATE,
        mvrwofexpires DATE,
        mvrodo INTEGER,
        mvrtotowners INTEGER,
        mvrcolourbase VARCHAR(255),
        mvrcoloursecondary VARCHAR(255),
        mvrregmake VARCHAR(255),
        mvrregmodel VARCHAR(255),
        mvrregsubmodel VARCHAR(255),
        submodelspec VARCHAR(255),
        createddateutc TIMESTAMP,
        lastupdatedutc TIMESTAMP,
        iscached BOOLEAN
      );
    `);
    console.log("Table created or already exists");
  } catch (error) {
    console.error("Error creating table:", error);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Main execution function
 * - Creates the table in the PostgreSQL database if it does not already exist
 * - Processes the large JSON file in batches
 * - Catches any errors and logs them to the console
 * - Closes the database connection before exiting
 */
async function main() {
  try {
    await createTableIfNotExists();
    await processLargeJSONFile();
  } catch (error) {
    console.error("Error in main execution:", error);
    await pool.end();
  }
}

main();