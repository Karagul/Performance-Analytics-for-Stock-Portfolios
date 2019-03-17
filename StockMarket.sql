-- creating a temporary table to aggregate data
CREATE TABLE public._temp_company_list
(
    symbol character varying(100) COLLATE pg_catalog."default",
    name character varying(100) COLLATE pg_catalog."default",
    last_sale character varying(100) COLLATE pg_catalog."default",
    market_cap character varying(100) COLLATE pg_catalog."default",
    ipo_year character varying(100) COLLATE pg_catalog."default",
    sector character varying(100) COLLATE pg_catalog."default",
    industry character varying(100) COLLATE pg_catalog."default",
    summary_quote character varying(100) COLLATE pg_catalog."default",
	blank character varying(100) COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public._temp_company_list
    OWNER to postgres;

-- Actual tables for data transfer
CREATE TABLE public.stock_mkt
(
    stock_mkt_name character varying(16) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT stock_mkt_pkey PRIMARY KEY (stock_mkt_name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.stock_mkt
    OWNER to postgres;


CREATE TABLE public.company_list
(
    symbol character varying(16) COLLATE pg_catalog."default" NOT NULL,
    stock_mkt_name character varying(16) COLLATE pg_catalog."default" NOT NULL,
    company_name character varying(100) COLLATE pg_catalog."default",
    market_cap_text character varying(100) COLLATE pg_catalog."default",
	sector character varying(100) COLLATE pg_catalog."default",
    industry character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT company_list_pkey PRIMARY KEY (symbol, stock_mkt_name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.company_list
    OWNER to postgres;


-- Setting the referential integrity (FK) constraints


ALTER TABLE public.company_list
    ADD CONSTRAINT company_list_fkey FOREIGN KEY (stock_mkt_name)
    REFERENCES public.stock_mkt (stock_mkt_name) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
CREATE INDEX fki_company_list_fkey
    ON public.company_list(stock_mkt_name);


-- Populate Tables with Data
INSERT INTO stock_mkt (stock_mkt_name) VALUES ('NASDAQ');

-- Load company_list with data stored in _temp_company_list

-- import NASDAQ data from csv
INSERT INTO company_list
SELECT symbol, 'NASDAQ' AS stock_mkt_name, name company_name, market_cap market_cap_text,industry,sector 
FROM _temp_company_list;
TRUNCATE TABLE _temp_company_list;

-- import NYSE data from csv
INSERT INTO company_list
SELECT symbol, 'NYSE' AS stock_mkt_name, name company_name, market_cap market_cap_text,industry,sector 
FROM _temp_company_list;
TRUNCATE TABLE _temp_company_list;

-- import AMEX data from csv
INSERT INTO company_list
SELECT symbol, 'AMEX' AS stock_mkt_name, name company_name, market_cap market_cap_text,industry,sector 
FROM _temp_company_list;
TRUNCATE TABLE _temp_company_list;


-- Dealing with n/a and leading/trailing blanks 

SELECT * FROM company_list LIMIT 10;
UPDATE company_list SET symbol=NULL WHERE symbol='n/a';
UPDATE company_list SET company_name=NULL WHERE company_name='n/a';
UPDATE company_list SET market_cap_text=NULL WHERE market_cap_text='n/a';
UPDATE company_list SET sector=NULL WHERE sector='n/a';
UPDATE company_list SET industry=NULL WHERE industry='n/a';

UPDATE stock_mkt SET stock_mkt_name=TRIM(stock_mkt_name);
UPDATE company_list SET 
	stock_mkt_name=TRIM(stock_mkt_name)
	,company_name=TRIM(company_name)
	,market_cap_text=TRIM(market_cap_text)
	,sector=TRIM(sector)
	,industry=TRIM(industry);

SELECT * FROM company_list LIMIT 10;

-- Extract market capitalization from text
SELECT *
	,CASE
     	WHEN "right"(btrim(market_cap_text), 1) = 'B' THEN 1000000000.0
        WHEN "right"(btrim(market_cap_text), 1) = 'M' THEN 1000000.0
	ELSE NULL::numeric
    END::double precision * "substring"(btrim(market_cap_text), 2, length(btrim(market_cap_text)) - 2)::double precision AS mkt_cap_usd
FROM company_list;

-- create a view with market capitalization 
CREATE OR REPLACE VIEW public.v_company_list AS
 SELECT company_list.symbol,
    company_list.stock_mkt_name,
    company_list.company_name,
    company_list.market_cap_text,
    company_list.sector,
    company_list.industry,
        CASE
            WHEN "right"(btrim(company_list.market_cap_text::text), 1) = 'B'::text THEN 1000000000.0
            WHEN "right"(btrim(company_list.market_cap_text::text), 1) = 'M'::text THEN 1000000.0
            ELSE NULL::numeric
        END::double precision * "substring"(btrim(company_list.market_cap_text::text), 2, length(btrim(company_list.market_cap_text::text)) - 2)::double precision AS mkt_cap_usd
   FROM company_list;

ALTER TABLE public.v_company_list
    OWNER TO postgres;
-- Check
SELECT * FROM v_company_list;

-- Create table eod_quotes and import Quandl Wiki csv into it

CREATE TABLE public.eod_quotes
(
    ticker character varying(16) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open real,
    high real,
    low real,
    close real,
    volume double precision,
    "ex.dividend" real,
    split_ration real,
    adj_open real,
    adj_high real,
    adj_low real,
    adj_close real,
    adj_volume double precision,
    CONSTRAINT eod_quotes_pkey PRIMARY KEY (ticker, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.eod_quotes
    OWNER to postgres;


-- Create table eod_indices and import SP500TR data into it
CREATE TABLE public.eod_indices
(
    symbol character varying(16) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open real,
    high real,
    low real,
    close real,
    adj_close real,
    volume double precision,
    CONSTRAINT eod_indices_pkey PRIMARY KEY (symbol, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.eod_indices
    OWNER to postgres;

-- prepare a custom calendar (using a spreadsheet) and import that data into the following table

CREATE TABLE public.custom_calendar
(
    date date NOT NULL,
    y bigint,
    m bigint,
    d bigint,
    dow character varying(3) COLLATE pg_catalog."default",
    trading smallint,
    CONSTRAINT custom_calendar_pkey PRIMARY KEY (date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.custom_calendar
    OWNER to postgres;


-- Add columns: eom (end-of-month) and prev_trading_day
ALTER TABLE public.custom_calendar
    ADD COLUMN eom smallint;
ALTER TABLE public.custom_calendar
    ADD COLUMN prev_trading_day date;

-- Identify and update the table with previous trading day info
UPDATE custom_calendar
SET prev_trading_day = PTD.ptd
FROM (SELECT date, (SELECT MAX(CC.date) FROM custom_calendar CC WHERE CC.trading=1 AND CC.date<custom_calendar.date) ptd FROM custom_calendar) PTD
WHERE custom_calendar.date = PTD.date;
-- CHECK
SELECT * FROM custom_calendar ORDER BY date;

-- Use the last trading day of 2011 (as the end of the month)
INSERT INTO custom_calendar VALUES('2011-12-30',2011,12,30,'Fri',1,1,NULL);
-- Re-run the update
UPDATE custom_calendar
SET prev_trading_day = PTD.ptd
FROM (SELECT date, (SELECT MAX(CC.date) FROM custom_calendar CC WHERE CC.trading=1 AND CC.date<custom_calendar.date) ptd FROM custom_calendar) PTD
WHERE custom_calendar.date = PTD.date;
-- CHECK again
SELECT * FROM custom_calendar ORDER BY date;

-- Identify and update the end-of-month column
UPDATE custom_calendar
SET eom = EOMI.endofm
FROM (SELECT CC.date,CASE WHEN EOM.y IS NULL THEN 0 ELSE 1 END endofm FROM custom_calendar CC LEFT JOIN
(SELECT y,m,MAX(d) lastd FROM custom_calendar WHERE trading=1 GROUP by y,m) EOM
ON CC.y=EOM.y AND CC.m=EOM.m AND CC.d=EOM.lastd) EOMI
WHERE custom_calendar.date = EOMI.date;
-- CHECK
SELECT * FROM custom_calendar ORDER BY date;
SELECT * FROM custom_calendar WHERE eom=1 ORDER BY date;

-- Create a role for the database  
CREATE USER stockmarketreader WITH
	LOGIN
	NOSUPERUSER
	NOCREATEDB
	NOCREATEROLE
	INHERIT
	NOREPLICATION
	CONNECTION LIMIT -1
	PASSWORD 'read123';

-- Grant read rights (on existing tables and views)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stockmarketreader;

-- Grant read rights (for future tables and views)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON TABLES TO stockmarketreader;
 