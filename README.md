# E-Commerce-Product-Insights-using-SQL
This project involves analyzing over 6,000 e-commerce user events using advanced SQL techniques to uncover actionable insights around funnel performance, sales trends, user segmentation, and customer value.

📊 Project Summary

Data Size: ~6,000 rows of event-level user data

Tools Used: MySQL, Excel

Objective: Understand user behavior, optimize the funnel, forecast revenue, and analyze A/B test group performance.

🔧 Technologies

SQL (MySQL)

Excel (for supporting dashboards and validations)

📁 Dataset Overview

Column Name

Description

user_id

Unique identifier for the user

session_id

Unique session identifier

event_time

Timestamp of the user event

event_type

Type of event (login, view, purchase, etc)

product_id

ID of the product interacted with

sales_amount

Amount spent on purchase events

gender

Gender of the user

device_type

Device used for session

traffic_source

Traffic source (Organic, Paid, etc.)

test_group

A/B test group label (control/test)

📈 Key Analyses Performed

🛒 Funnel Analysis

Count of users at each stage: login → view → add_to_cart → checkout → purchase

Drop-off percentages between stages

Funnel behavior segmented by device, gender, and age

⏱️ Session Analysis

Session duration by:

Event type

Device

Age group

User segment (new/returning)

📅 Sales Forecasting

7-day and 30-day linear regression forecasting

Moving averages (7-day & 30-day)

📦 Product Insights

Top and bottom performing products by revenue

Category-wise sales analysis

📊 User Segmentation

RFM Segmentation (Recency, Frequency, Monetary)

Segmentation by spend levels

💰 Revenue Metrics

Monthly and weekly sales trends

ARPU (Average Revenue per User)

LTV (Customer Lifetime Value) modeling

City-wise user and revenue distribution

🧪 A/B Test Analysis

Group-wise user counts

Conversion rates: test (31.07%) vs control (30.74%)

Total and average revenue by group

📌 Key Outcomes

Identified funnel drop-offs that helped improve user journey

Predicted sales trends using linear regression models

Guided marketing strategy with test vs control group comparison

Implemented RFM segmentation to classify customers and understand value tiers

🧠 Learnings

SQL window functions for segmentation and cohorting

Linear regression using SQL aggregation (no ML library required)

Business KPIs like ARPU, LTV, and conversion lift
