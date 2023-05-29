import pandas as pd
from sodapy import Socrata
from config import app_token, username, password

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.lacity.org", None)

# Example authenticated client (needed for non-public datasets):
client = Socrata("data.lacity.org",
                 app_token,
                 username=username,
                 password=password)

# results returned as JSON from API / converted to Python list of dictionaries by sodapy.
results = client.get("wjz9-h9np", limit=20, select="ticket_number, issue_date")

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

print(results_df.head())