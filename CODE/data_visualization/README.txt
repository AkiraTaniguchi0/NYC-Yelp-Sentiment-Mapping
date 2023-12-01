TeamBD (#110)

Description:
This folder contains the final Tableau workbook, data extract, and data files required for a live Tableau connection. The final dashboard is published on Tableau public (https://public.tableau.com/app/profile/jay.nonaka/viz/RestaurantReviewSentimentDashboard/MainDash?publish=yes)

Installation:
1. Install Tableau Desktop (https://www.tableau.com/products/desktop)
2. Create/Login to Tableau Public account (https://www.tableau.com/tableau-login-hub)

Execution:
1. Tableau Desktop
Open 'Restaurant Review Sentiment Dashboard.twb' and connect to the data extract file ('data_extract.hyper'). If making a live connection, establish connections with the following files:
- restaurants.xlsx
- reviews_nlp.csv
- geo_export_763a711e-db4a-4497-92de-844b1d1943dc.shp 

NOTE: The other files in the 'Borough Boundaries' folder do not need to be connected. However removing these files from the folder will disable the Tableau visualization.

2. Tableau Public
The dashboard is published to Tableau Public and can be accessed via this link (https://public.tableau.com/app/profile/jay.nonaka/viz/RestaurantReviewSentimentDashboard/MainDash). Users who wish to make changes and re-publish the workbook can do so by following these steps (https://help.tableau.com/current/pro/desktop/en-us/publish_workbooks_tableaupublic.htm).