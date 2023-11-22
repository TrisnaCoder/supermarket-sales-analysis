CREATE TABLE table_m3 (
    "Invoice ID" text,
    "Branch" text,
    "City" text,
    "Customer type" text,
    "Gender" text,
    "Product line" text,
    "Unit price" double precision,
    "Quantity" integer,
    "Tax 5%" double precision,
    "Total" double precision,
    "Date" date,
    "Time" time without time zone,
    "Payment" text,
    "cogs" double precision,
    "gross margin percentage" double precision,
    "gross income" double precision,
    "Rating" double precision
);

COPY table_m3("Invoice ID", "Branch", "City", "Customer type", "Gender", "Product line", "Unit price", "Quantity", "Tax 5%", "Total", "Date", "Time", "Payment", "cogs", "gross margin percentage", "gross income", "Rating") 
FROM 'C:\Users\user\Downloads\P2M3_trisna_data_raw.csv'
DELIMITER ','
CSV HEADER;



