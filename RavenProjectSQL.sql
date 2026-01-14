-- create database
create database ravenstack_analytics;

use ravenstack_analytics;

CREATE TABLE raw_accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    account_name VARCHAR(100),
    industry VARCHAR(50),
    country CHAR(2),
    signup_date DATE,
    referral_source VARCHAR(30),
    plan_tier VARCHAR(30),
    seats INT,
    is_trial VARCHAR(10),
    churn_flag VARCHAR(10)
);

CREATE TABLE raw_subscriptions (
    subscription_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50),
    start_date DATE,
    end_date VARCHAR(20),  --
    plan_tier VARCHAR(30),
    seats INT,
    mrr_amount DECIMAL(10,2),
    arr_amount DECIMAL(10,2),
    is_trial VARCHAR(10),
    upgrade_flag VARCHAR(10),
    downgrade_flag VARCHAR(10),
    churn_flag VARCHAR(10),
    billing_frequency VARCHAR(20),
    auto_renew_flag VARCHAR(10),
    FOREIGN KEY (account_id) REFERENCES raw_accounts(account_id)
);

CREATE TABLE raw_feature_usage (
    usage_id VARCHAR(50) PRIMARY KEY,
    subscription_id VARCHAR(50),
    usage_date DATE,
    feature_name VARCHAR(100),
    usage_count INT,
    usage_duration_secs INT,
    error_count INT,
    is_beta_feature VARCHAR(10),
    FOREIGN KEY (subscription_id) REFERENCES raw_subscriptions(subscription_id)
);


CREATE TABLE raw_support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50),
    submitted_at DATETIME,
    closed_at DATETIME,
    resolution_time_hours DECIMAL(5,2),
    priority VARCHAR(20),
    first_response_time_minutes INT,
    satisfaction_score INT,
    escalation_flag VARCHAR(10),
    FOREIGN KEY (account_id) REFERENCES raw_accounts(account_id)
);


CREATE TABLE raw_churn_events (
    churn_event_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50),
    churn_date DATE,
    reason_code VARCHAR(50),
    refund_amount_usd DECIMAL(10,2),
    preceding_upgrade_flag VARCHAR(10),
    preceding_downgrade_flag VARCHAR(10),
    is_reactivation VARCHAR(10),
    feedback_text TEXT,
    FOREIGN KEY (account_id) REFERENCES raw_accounts(account_id)
);


select count(*) from raw_accounts;
select count(distinct account_id) from raw_accounts;
select * from raw_subscriptions limit 10;
select plan_tier, count(*) from raw_subscriptions group by plan_tier;
select count(*) from raw_churn_events;

create schema if not exists clean;

CREATE OR REPLACE VIEW clean.accounts AS
SELECT
    account_id,
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    plan_tier,
    seats,
    CASE 
        WHEN is_trial = 'True' THEN 1 
        ELSE 0 
    END AS is_trial,
    CASE 
        WHEN churn_flag = 'True' THEN 1 
        ELSE 0 
    END AS churn_flag
FROM raw_accounts;

select * from clean.accounts;

CREATE OR REPLACE VIEW clean.subscriptions AS
SELECT 
	subscription_id,
	account_id,
	start_date,
    case 
		when end_date = '' then null 
        else cast(end_date as date)
	end as end_date,
	plan_tier,
	seats,
	mrr_amount,
	arr_amount,
	billing_frequency,
	CASE
		WHEN is_trial = 'True' THEN 1
		ELSE 0
	END AS is_trial,
	CASE
		WHEN upgrade_flag = 'True' THEN 1
		ELSE 0
	END AS upgrade_flag,
	CASE
		WHEN downgrade_flag = 'True' THEN 1
		ELSE 0
	END AS downgrade_flag,
	CASE
		WHEN churn_flag = 'True' THEN 1
		ELSE 0
	END AS churn_flag,
	CASE
		WHEN auto_renew_flag = 'True' THEN 1
		ELSE 0
	END AS auto_renew_flag
FROM raw_subscriptions;

select * from clean.subscriptions;


CREATE OR REPLACE VIEW clean.feature_usage AS
    SELECT 
        usage_id,
        subscription_id,
        usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        CASE
            WHEN is_beta_feature = 'True' THEN 1
            ELSE 0
        END AS is_beta_feature
    FROM
        raw_feature_usage;


create or replace view clean.support_tickets as
	select
		ticket_id,
		account_id, 
		submitted_at,
		closed_at, 
		resolution_time_hours, 
		priority, 
		first_response_time_minutes,
		satisfaction_score, 
		case
			when escalation_flag = 'True' then 1
            else 0
		end as escalation_flag
	from 
		raw_support_tickets;
        
select * from clean.support_tickets;

CREATE OR REPLACE VIEW clean.churn_events AS
    SELECT 
        churn_event_id,
        account_id,
        churn_date,
        reason_code,
        refund_amount_usd,
        feedback_text,
        CASE
            WHEN preceding_upgrade_flag = 'True' THEN 1
            ELSE 0
        END AS preceding_upgrade_flag,
        CASE
            WHEN preceding_downgrade_flag = 'True' THEN 1
            ELSE 0
        END AS preceding_downgrade_flag,
        CASE
            WHEN is_reactivation = 'True' THEN 1
            ELSE 0
        END AS is_reactivation
    FROM
        raw_churn_events;
        
select * from clean.churn_events;


SELECT COUNT(*) FROM clean.accounts;
SELECT COUNT(*) FROM clean.subscriptions;
SELECT COUNT(*) FROM clean.feature_usage;
SELECT COUNT(*) FROM clean.support_tickets;
SELECT COUNT(*) FROM clean.churn_events;


CREATE TABLE clean.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(15),
    quarter INT
);

INSERT INTO clean.dim_date
SELECT DISTINCT
    d AS date_id,
    YEAR(d),
    MONTH(d),
    MONTHNAME(d),
    QUARTER(d)
FROM (
    SELECT signup_date AS d FROM clean.accounts
    UNION
    SELECT start_date FROM clean.subscriptions
    UNION
    SELECT end_date FROM clean.subscriptions WHERE end_date IS NOT NULL
    UNION
    SELECT usage_date FROM clean.feature_usage
    UNION
    SELECT churn_date FROM clean.churn_events
) x;



CREATE TABLE clean.dim_accounts AS
SELECT DISTINCT
    account_id,
    industry,
    country,
    referral_source
FROM clean.accounts;



CREATE TABLE clean.dim_plans AS
SELECT DISTINCT
    plan_tier,
    billing_frequency
FROM clean.subscriptions;


CREATE TABLE clean.fact_subscriptions AS
SELECT
    s.subscription_id,
    s.account_id,
    s.plan_tier,
    s.start_date,
    s.end_date,
    s.seats,
    s.mrr_amount,
    s.arr_amount,
    s.is_trial,
    s.upgrade_flag,
    s.downgrade_flag,
    s.churn_flag
FROM clean.subscriptions s;


CREATE TABLE clean.fact_feature_usage AS
SELECT
    usage_id,
    subscription_id,
    usage_date,
    usage_count,
    usage_duration_secs,
    error_count,
    is_beta_feature
FROM clean.feature_usage;



CREATE TABLE clean.fact_support AS
SELECT
    ticket_id,
    account_id,
    submitted_at,
    resolution_time_hours,
    priority,
    first_response_time_minutes,
    escalation_flag
FROM clean.support_tickets;


CREATE TABLE clean.fact_churn AS
SELECT
    churn_event_id,
    account_id,
    churn_date,
    reason_code,
    refund_amount_usd,
    preceding_upgrade_flag,
    preceding_downgrade_flag,
    is_reactivation
FROM clean.churn_events;


SELECT COUNT(*) FROM clean.fact_subscriptions;
SELECT COUNT(*) FROM clean.fact_feature_usage;
SELECT COUNT(*) FROM clean.fact_support;
SELECT COUNT(*) FROM clean.fact_churn;

    





