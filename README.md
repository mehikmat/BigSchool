http://hortonworks.com/hadoop-tutorial/how-to-analyze-machine-and-sensor-data/#lab-1

1. Analyze Machine and Sensor Data:
   To monitor infrastructure or machines such as ventilation equipment,airplane engines.
   This data can be used for predictive analytics, to repair or replace these items before they break.

   Eg. Processing sensor data from building operations:
       we will refine and analyze the data from Heating, Ventilation, Air Conditioning (HVAC) systems in 20 buildings.
       Since one building may be utilizing chilled water for air conditioning and the warm water it returns may be
       used in another building for heating.

       Now,
       hvac.csv –     Contains targeted and actual building temperatures.
                      The Apache Flume can be used to obtain building temperature data.
       building.csv – Contains the “building” database table.
                      Apache Sqoop can be used to transfer this type of data from a structured database into HFDS.
       Run hadoop job to refine the sensor data and to analyse.
       Access the refined/processed sensor data custom application.

2. Analyzing Social Media and Customer Sentiment:
   - Post a facebook post "Guys, what's on your mind?"
   - Get the all the comments
   - Categorize the comments; positive,negative,neutral (sentiment analysis)
   - Summarize and predict whether the comments are negative or positive or neutral.

   The Apache OpenNLP Document Categorizer can be used to classify text into pre-defined categories.

