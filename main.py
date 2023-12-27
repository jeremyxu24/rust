'''
Attributes
Identifying Attributes
Metadata Attributes
'''
import subprocess
import ast
import pandas as pd
import json
import csv
import re
from typing import Any, Optional
from dataclasses import asdict, dataclass, field, is_dataclass, replace

from pathlib import Path

root = Path(__file__).parent.resolve()

import time 

@dataclass
class InputAddress:
    street: Optional[str]
    zipcode: Optional[str]


IP_PATTERN = re.compile(r'^([0-9]{3}\.){3}[0-9]{3}$')
EMAIL_PATTERN = re.compile(r'^[^@]+@[^\.]{2,}\..{2,}$')

@dataclass
class InputRecord:
    first_names: list[Optional[str]]
    last_names: list[Optional[str]]
    addresses: list[Optional[InputAddress]]
    phone_numbers: list[Optional[str]]
    ips: list[Optional[str]]
    emails: list[Optional[str]]
    extras: dict[str, Any] = field(default_factory=dict)

    def clean(self) -> 'CleanedRecord':
        return CleanedRecord(
            first_names=self._clean_names(self.first_names),
            last_names=self._clean_names(self.last_names),
            addresses=self._clean_addresses(self.addresses),
            phone_numbers=self._clean_phone_numbers(self.phone_numbers),
            emails=self._clean_emails(self.emails),
            ips=self._clean_ips(self.ips),
            extras=self.extras,
        )
    
    @staticmethod
    def _clean_names(first_names: list[Optional[str]]) -> list[str]:
        return [
            trimmed
            for fn in first_names
            if fn is not None
            and (trimmed := fn.strip().lower()) != ''
        ]
    
    @staticmethod
    def _clean_addresses(addresses: list[Optional[InputAddress]]) -> list['CleanedAddress']:
        return [
            CleanedAddress(
                street=street_trimmed,
                zipcode=zip_trimmed,
            )
            for addr in addresses
            if addr.street is not None
            and (street_trimmed := addr.street.strip()) != ''
            and addr.zipcode is not None
            and (zip_trimmed := addr.zipcode.strip()) != ''
        ]
    
    @staticmethod
    def _clean_phone_numbers(phone_numbers: list[Optional[str]]) -> list[str]:
        # also validate for actual phone number (e.g., no 18005551234)
        # check for high frequency phone numbers
        return [
            f'+1{subbed}' if len(subbed) == 10 else f'+{subbed}'
            for pn in phone_numbers
            if pn is not None
            and (trimmed := pn.strip()) != ''
            and (subbed := re.sub(r'[^0-9]', '', trimmed)) != ''
            and (
                len(subbed) == 10
                or (
                    len(subbed) == 11
                    and subbed.startswith('1')
                )
            )
        ]
    
    @staticmethod
    def _clean_ips(ips: list[Optional[str]]) -> list[str]:
        return [
            trimmed
            for ip in ips
            if ip is not None
            and (trimmed := ip.strip()) != ''
            and IP_PATTERN.match(trimmed) is not None
        ]
    
    @staticmethod
    def _clean_emails(emails: list[Optional[str]]) -> list[str]:
        # check for high frequency emails
        return [
            trimmed
            for email in emails
            if email is not None
            and (trimmed := email.strip().lower()) != ''
            and EMAIL_PATTERN.match(trimmed) is not None
        ]


@dataclass
class CleanedAddress:
    street: str
    zipcode: str


@dataclass
class CleanedRecord:
    first_names: list[str]
    last_names: list[str]
    addresses: list[CleanedAddress]
    phone_numbers: list[str]
    ips: list[str]
    emails: list[str]
    extras: dict[str, Any] = field(default_factory=dict)




def read_sql_file(file_path):
    try:
        with open(file_path, 'r') as sql_file:
            # Read the entire contents of the file
            sql_content = sql_file.read()
            return sql_content
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

def main():
    # This CSV is used to simulate getting unique list from the database
    df = pd.read_csv('simulated_query_results.csv')

    # Process each row in the DataFrame
    records = []
    for row in df.itertuples(index=False):
        # Ensure the row is not empty
        if row[1:]:
            customer_id, *data = row
            try:
                first_names, last_names, addresses, phone_numbers, ips, emails = [json.loads(entry) for entry in data]
            except json.decoder.JSONDecodeError:
                print(f"Skipping invalid JSON in row with customer_id: {customer_id}")
                continue

            record = InputRecord(
                first_names=first_names,
                last_names=last_names,
                addresses=[InputAddress(*address) for address in addresses],
                phone_numbers=phone_numbers,
                ips=ips,
                emails=emails,
                extras={'customer_id': customer_id},
            )
            records.append(record)

    cleaned_records = [record.clean() for record in records]

    return cleaned_records

class Encoder(json.encoder.JSONEncoder):
    def default(self, o: Any) -> Any:
        if is_dataclass(o):
            return asdict(o)
        else:
            return super().default(o)



if __name__ == '__main__':
    # data = main()
    csv_file_path = './lead_match/new.csv'
    # with open(csv_file_path, mode='w', newline='') as csv_file:
    #     fieldnames = [
    #         'First Name', 'Last Name', 'Street Address', 'Zip Code',
    #         'Phone Number', 'IP Address', 'Email', 'Customer ID'
    #     ]
    #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    #     writer.writeheader()

    #     for record in data:
    #         row_data = {
    #             'First Name': record.first_names[0].title() if record.first_names else None,
    #             'Last Name': record.last_names[0].title() if record.last_names else None,
    #             'Street Address': record.addresses[0].street if record.addresses else None,
    #             'Zip Code': record.addresses[0].zipcode if record.addresses else None,
    #             'Phone Number': record.phone_numbers[0] if record.phone_numbers else None,
    #             'IP Address': record.ips[0] if record.ips else None,
    #             'Email': record.emails[0] if record.emails else None,
    #             'Customer ID': record.extras['customer_id'] if record.extras['customer_id'] else None,
    #         }
    #         writer.writerow(row_data)

    # print(f"Data written to {csv_file_path}")
    
    rust_project_path = r'./lead_match'
    start_time = time.time() 
    try:
        print("starting test now...")
        # Use 'cargo run' to build and execute your Rust project
        result = subprocess.run(['cargo', 'run'], cwd=rust_project_path, capture_output=True, text=True, check=True)
        
        # Split the captured output into lines
        output_lines = result.stdout.split('\n')[0].split("|")
        for i, element in enumerate(output_lines):
            if i%2==0:
                print(f"Subset {int(((i)/2))}:")
            if "matches:" in element and "no" not in element:
                matches = ast.literal_eval(element.split(":")[1])
                print("Matches",matches)
            if "no_matches:" in element:
                no_matches = ast.literal_eval(element.split(":")[1])
                print("No Matches",no_matches)
            print("\n")


        # no_matches = ast.literal_eval(output_lines[1].replace("no_matches",""))
        # new_leads_list = pd.read_csv(csv_file_path)
        # print(f"Matches: {len(matches)}")
        # print(f"No matches: {len(matches)}")
        # print("Matches:")
        # print(new_leads_list.iloc[matches])
        # print("No Matches:")
        # print(new_leads_list.iloc[no_matches])
    except subprocess.CalledProcessError as e:
        print(f"Error running Rust project: {e}")

    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time in seconds
    print(f"Execution time: {execution_time:.2f} seconds")
