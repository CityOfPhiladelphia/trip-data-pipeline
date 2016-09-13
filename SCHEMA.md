In the database, first create the taxi_trips table:

    CREATE TABLE taxi_trips (
      trip_no VARCHAR(16),
      operator_name NVARCHAR2(2000),
      medallion VARCHAR(16),
      chauffeur_no VARCHAR(16),
      meter_on_datetime VARCHAR(24),
      meter_off_datetime VARCHAR(24),
      trip_length VARCHAR(16),
      pickup_latitude VARCHAR(16),
      pickup_longitude VARCHAR(16),
      pickup_location NVARCHAR2(2000),
      dropoff_latitude VARCHAR(16),
      dropoff_longitude VARCHAR(16),
      dropoff_location NVARCHAR2(2000),
      fare VARCHAR(16),
      tax VARCHAR(16),
      tips VARCHAR(16),
      tolls VARCHAR(16),
      surcharge VARCHAR(16),
      trip_total VARCHAR(16),
      payment_type VARCHAR(16),
      street_or_dispatch VARCHAR(32),
      data_source VARCHAR(8),
      geom sde.ST_GEOMETRY
    )

Also, create an index on what is a maximal unique identifier for taxi trips; we
use it for upsertig records into the trips table:

    CREATE INDEX taxi_trip_unique_id ON taxi_trips (
      Trip_No, Medallion, Chauffeur_No, Meter_On_Datetime, Meter_Off_Datetime
    )

For anonymizing chauffeur and medallion numbers, create two tables to maintain a
mapping from actual Medallion and Chauffeur numbers to arbitrary identifiers.
With Oracle 12c+, use the following SQL:

    CREATE SEQUENCE chauffeur_no_seq;
    CREATE TABLE chauffeur_no_ids (
      ID           NUMBER DEFAULT chauffeur_no_seq.NEXTVAL,
      Chauffeur_No VARCHAR2(16)
    );

    CREATE SEQUENCE medallion_seq;
    CREATE TABLE medallion_ids (
      ID        NUMBER DEFAULT medallion_seq.NEXTVAL,
      Medallion VARCHAR2(16)
    );

For Oracle pre-12c, use the following:

    CREATE TABLE chauffeur_no_ids (
      ID            NUMBER         NOT NULL,
      Chauffeur_No  VARCHAR2(16) NOT NULL);
    CREATE INDEX chauffeur_no_idx ON chauffeur_no_ids (Chauffeur_No)
    CREATE SEQUENCE chauffeur_no_seq;
    CREATE OR REPLACE TRIGGER chauffeur_no_trig
    BEFORE INSERT ON chauffeur_no_ids
    FOR EACH ROW
    BEGIN
      SELECT chauffeur_no_seq.NEXTVAL
      INTO   :new.ID
      FROM   dual;
    END;

    CREATE TABLE medallion_ids (
      ID         NUMBER         NOT NULL,
      Medallion  VARCHAR2(16) NOT NULL);
    CREATE INDEX medallion_idx ON medallion_ids (Medallion)
    CREATE SEQUENCE medallion_seq;
    CREATE OR REPLACE TRIGGER medallion_trig
    BEFORE INSERT ON medallion_ids
    FOR EACH ROW
    BEGIN
      SELECT medallion_seq.NEXTVAL
      INTO   :new.ID
      FROM   dual;
    END;

Finally, for the public, create the following view:

    -- TODO: Round times to nearest 15 minutes

    CREATE MATERIALIZED VIEW anonymized_taxi_trips AS
        SELECT Trip_No, Operator_Name,
            mids.ID AS Medallion_ID,
            cnids.ID AS Chauffeur_ID,
            Meter_On_Datetime, Meter_Off_Datetime,
            Trip_Length,
            Pickup_Latitude, Pickup_Longitude, Pickup_Location,
            Dropoff_Latitude, Dropoff_Longitude, Dropoff_Location,
            Fare, Tax, Tips, Tolls, Surcharge, Trip_Total,
            Payment_Type,
            Street_or_Dispatch,
            Data_Source,
            sde.ST_GeomFromText ('MULTIPOINT ('||Pickup_Longitude||' '||Pickup_Latitude||', '||Dropoff_Longitude||' '||Dropoff_Latitude||')', 4326) AS Pickup_Dropoff
        FROM taxi_trips ts
        LEFT JOIN medallion_ids mids ON ts.Medallion = mids.Medallion
        LEFT JOIN chauffeur_no_ids cnids ON ts.Chauffeur_No = cnids.Chauffeur_No;

