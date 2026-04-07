# Data Engineering Assignment

## Goal

In this exercise, you will play the role of a Data Engineer working with our business analysts to analyse an open-source dataset and build a data mart to answer some queries. You are asked to design and implement a cloud-based data processing architecture using AWS to solve the requirements described below. The answers for this activity will allow the business analyst to better understand how this data can be integrated with our current data and derive value.

This task will allow us to judge your ability to analyse new source systems, and perform data profiling, data cleaning, data modelling and pipeline development skills.

### Data Set

OpenAQ is a non-profit organisation that curates a data set of global, aggregated physical air quality measurements provided by governments and other sources. OpenAQ publishes its data to the AWS open data registry maintaining historical measurements in an S3 bucket and broadcasting new measurements in real time on an SNS topic. For now, you may load this single file with the assumption that your ingestion pipeline will take care of all history and future loads.

👉 [OpenAQ Dataset](https://openaq-data.s3.amazonaws.com/index.html)

---

## Business Requirements

Build a data mart hosted in Redshift (if you don't have Redshift use any database you are familiar with) to enable BA to perform analysis on historical 2017 air quality measurements.

### Queries
The data mart must support the following analytic queries in a timely and cost‑optimised manner:

- **Monthly Analysis**  
  For any given month find all cities with average monthly levels of carbon monoxide (CO) and sulphur dioxide pollution (SO₂) in the 90th percentile globally.

- **Daily Top Cities**  
  For any given day find the top 5 cities globally with the highest daily average levels of particle air pollution (PM2.5).

- **Hourly Analysis**  
  For any given hour find the top 10 cities with the highest daily average levels of particle air pollution (PM2.5) and provide the mean, median and mode measures of carbon monoxide (CO) and sulphur dioxide pollution (SO₂) for those cities on that day.

- **Air Quality Index**  
  For any given hour report an air quality index for each country. While you are free to compute this index in the manner that you see fit, it must take into account measures of particle air pollution, sulphur dioxide pollution, and carbon monoxide pollution. The index must have 3 discrete levels (high, moderate, and low).

---

### Data Quality
Your data mart should contain valid data only:
- Exclude measurements that will adversely impact the accuracy of your analysis.  
- Provide a mechanism to report on bad data (reports run infrequently on an ad‑hoc basis).  
- Where timely data points are unavailable, interpolate as appropriate.  

---

## Non-functional Requirements

- **Scalability**: While your prototype will only use OpenAQ data from 2017 in the first instance, your design must be able to scale to terabytes of historical data from multiple additional sources.  
- **Quality**: The BA expects your prototype to provide the basis of a production‑grade solution with minimal re‑work. Your code should be readable, testable, and maintainable.  

---

## Deliverables

Propose an architecture to deploy the solution on AWS. Your solution should include the following:

- An adequate explanation of your solution alongside an architecture diagram as well as the Database schema.  
- Source code (short comments are appreciated). You can use a programming language of your choice (**Python Preferred**).  
- All SQL queries to deliver the required data by BA.  
- Any key configurations that would help explain your solution (screenshots would suffice where appropriate).  

---

**All the best!**
