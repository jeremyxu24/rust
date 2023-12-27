import random
import pandas as pd
from faker import Faker

# Create a Faker instance
fake = Faker()

# Generate fake data for 500,000 people
num_people = 10000
people = []
num_sets = 4

for i in range(num_people):
    person = {
        'First Name': fake.first_name(),
        'Last Name': fake.last_name(),
        'Street Address': fake.street_address(),
        'Zip Code': random.randint(10000, 99999),
        'Phone Number': fake.phone_number(),
        'IP Address': fake.ipv4(),
        'Email': fake.email(),
        'Customer ID': random.randint(1000, 9999),
    }
    people.append(person)

# Convert the data to a Pandas DataFrame
df = pd.DataFrame(people)

# Define the CSV file paths
existing_csv_file_path = './lead_match/existing.csv'
new_csv_file_path = './lead_match/new.csv'

# Split the data into four parts
total_rows = len(df)
quarter_size = total_rows // num_sets
existing_data_parts = []

for i in range(num_sets):
    start_idx = i * quarter_size
    end_idx = (i + 1) * quarter_size if i < num_sets else total_rows
    existing_data_part = df.iloc[start_idx:end_idx]
    existing_data_parts.append(existing_data_part)
    part_file_path = f'./lead_match/existing_{i + 1}.csv'
    existing_data_part.to_csv(part_file_path, index=False)

# Filter and limit the data for 'new.csv'
new_amount = round(num_people/100)
new_data = df.tail(new_amount)
new_data.to_csv(new_csv_file_path, index=False)

print(f"CSV files 'existing.csv' and 'new.csv' have been created.")
