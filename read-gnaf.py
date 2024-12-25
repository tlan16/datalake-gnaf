import time

import daft

def main():
    data_directory = get_data_directory("address")
    init()
    df = daft.read_deltalake(data_directory)
    print(f"Read {df.count_rows()} rows frame from {data_directory}")
    lookup_rea_office(df)

def lookup_rea_office(df: daft.DataFrame):
    from daft.sql import SQLCatalog
    from pprint import pprint
    timestamp_start = time.time_ns()

    catalog = SQLCatalog({"addresses": df})
    filtered = daft.sql("""
        SELECT * FROM addresses 
            WHERE STREET_TYPE_CODE = 'STREET'
            AND LOCALITY_NAME = 'RICHMOND'
            AND STATE_ABBREVIATION = 'VIC'
            AND POSTCODE = '3121'
            AND NUMBER_FIRST = 511
            AND ALIAS_PRINCIPAL = 'P'
            AND PRIMARY_SECONDARY = 'P'
    """, catalog=catalog)
    pprint(filtered.to_pylist())
    print(f"Query took: {(time.time_ns() - timestamp_start) / 1_000_000}ms")

def init():
    daft.context.set_runner_native()

def get_data_directory(data_type: str):
    import os
    return os.path.join(os.getcwd(), f"data/{data_type}")

if __name__ == "__main__":
    main()
