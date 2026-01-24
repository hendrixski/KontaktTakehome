CREATE TABLE IF NOT EXISTS anonymized_patient_records (
    uuid UUID PRIMARY KEY NOT NULL,
    favorite_color VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS patient_uuid_cache (
    name TEXT NOT NULL,
    dob DATE NOT NULL,
    uuid UUID NOT NULL,
    PRIMARY KEY (name, dob)
);
