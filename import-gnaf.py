def main():
    data_directory = get_data_directory("address")
    init(data_directory)
    read_address_detail(data_directory)

def init(data_directory: str):
    import daft
    import os
    if os.path.exists(data_directory):
        from shutil import rmtree
        rmtree(data_directory)
    daft.context.set_runner_native()

def read_address_detail(data_directory: str):
    import daft
    from daft import DataType

    df_address_detail = None
    other_states = ["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"]
    for state in other_states:
        print(f"Reading {state} address detail")
        df_state_address_detail = daft.read_csv(
            path=f"data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Standard/{state}_ADDRESS_DETAIL_psv.psv",
            delimiter='|',
            schema={
                "ADDRESS_DETAIL_PID": DataType.string(),
                "DATE_CREATED": DataType.date(),
                "DATE_LAST_MODIFIED": DataType.date(),
                "DATE_RETIRED": DataType.date(),
                "BUILDING_NAME": DataType.string(),
                "LOT_NUMBER_PREFIX": DataType.string(),
                "LOT_NUMBER": DataType.int16(),
                "LOT_NUMBER_SUFFIX": DataType.string(),
                "FLAT_TYPE_CODE": DataType.string(),
                "FLAT_NUMBER_PREFIX": DataType.string(),
                "FLAT_NUMBER": DataType.int16(),
                "FLAT_NUMBER_SUFFIX": DataType.string(),
                "LEVEL_TYPE_CODE": DataType.string(),
                "LEVEL_NUMBER_PREFIX": DataType.string(),
                "LEVEL_NUMBER": DataType.int8(),
                "LEVEL_NUMBER_SUFFIX": DataType.string(),
                "NUMBER_FIRST_PREFIX": DataType.string(),
                "NUMBER_FIRST": DataType.int32(),
                "NUMBER_FIRST_SUFFIX": DataType.string(),
                "NUMBER_LAST_PREFIX": DataType.string(),
                "NUMBER_LAST": DataType.int32(),
                "NUMBER_LAST_SUFFIX": DataType.string(),
                "STREET_LOCALITY_PID": DataType.string(),
                "LOCATION_DESCRIPTION": DataType.string(),
                "LOCALITY_PID": DataType.string(),
                "ALIAS_PRINCIPAL": DataType.string(),
                "POSTCODE": DataType.string(),
                "PRIVATE_STREET": DataType.string(),
                "LEGAL_PARCEL_ID": DataType.string(),
                "CONFIDENCE": DataType.int8(),
                "ADDRESS_SITE_PID": DataType.string(),
                "LEVEL_GEOCODED_CODE": DataType.int8(),
                "PROPERTY_PID": DataType.string(),
                "GNAF_PROPERTY_PID": DataType.string(),
                "PRIMARY_SECONDARY": DataType.string(),
            }
        )
        print(f"Reading {state} street locality")
        df_state_street_locality = daft.read_csv(
            path=f"data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Standard/{state}_STREET_LOCALITY_psv.psv",
            delimiter='|',
            schema={
                "STREET_LOCALITY_PID": DataType.string(),
                "DATE_CREATED": DataType.date(),
                "DATE_RETIRED": DataType.date(),
                "STREET_CLASS_CODE": DataType.string(),
                "STREET_NAME": DataType.string(),
                "STREET_TYPE_CODE": DataType.string(),
                "STREET_SUFFIX_CODE": DataType.string(),
                "LOCALITY_PID": DataType.string(),
                "GNAF_STREET_PID": DataType.string(),
                "GNAF_STREET_CONFIDENCE": DataType.int8(),
                "GNAF_RELIABILITY_CODE": DataType.int8(),
            }
        ).with_columns(
            {
                "STREET_LOCALITY_DATE_CREATED": daft.col("DATE_CREATED"),
                "STREET_LOCALITY_DATE_RETIRED": daft.col("DATE_RETIRED"),
                "STREET_LOCALITY_STREET_NAME": daft.col("STREET_NAME"),
                "STREET_LOCALITY_LOCALITY_PID": daft.col("LOCALITY_PID"),
                "STREET_LOCALITY_GNAF_STREET_PID": daft.col("GNAF_STREET_PID"),
                "STREET_LOCALITY_GNAF_STREET_CONFIDENCE": daft.col("GNAF_STREET_CONFIDENCE"),
                "STREET_LOCALITY_GNAF_RELIABILITY_CODE": daft.col("GNAF_RELIABILITY_CODE"),
            }
        ).exclude(
            "DATE_CREATED",
            "DATE_RETIRED",
            "STREET_NAME",
            "LOCALITY_PID",
            "GNAF_STREET_PID",
            "GNAF_STREET_CONFIDENCE",
            "GNAF_RELIABILITY_CODE",
        )
        print(f"Reading {state} locality")
        df_state_locality = daft.read_csv(
            path=f"data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Standard/{state}_LOCALITY_psv.psv",
            delimiter='|',
            schema={
                "LOCALITY_PID": DataType.string(),
                "DATE_CREATED": DataType.date(),
                "DATE_RETIRED": DataType.date(),
                "LOCALITY_NAME": DataType.string(),
                "PRIMARY_POSTCODE": DataType.string(),
                "LOCALITY_CLASS_CODE": DataType.string(),
                "STATE_PID": DataType.string(),
                "GNAF_LOCALITY_PID": DataType.string(),
                "GNAF_RELIABILITY_CODE": DataType.int8(),
            }
        ).with_columns(
            {
                "LOCALITY_DATE_CREATED": daft.col("DATE_CREATED"),
                "LOCALITY_DATE_RETIRED": daft.col("DATE_RETIRED"),
                "LOCALITY_PRIMARY_POSTCODE": daft.col("PRIMARY_POSTCODE"),
                "LOCALITY_GNAF_LOCALITY_PID": daft.col("GNAF_LOCALITY_PID"),
                "LOCALITY_GNAF_RELIABILITY_CODE": daft.col("GNAF_RELIABILITY_CODE"),
            }
        ).exclude(
            "DATE_CREATED",
            "DATE_RETIRED",
            "PRIMARY_POSTCODE",
            "GNAF_LOCALITY_PID",
            "GNAF_RELIABILITY_CODE",
        )
        print(f"Reading {state} address default geocode")
        df_state_address_default_geocode = daft.read_csv(
            path=f"data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Standard/{state}_ADDRESS_DEFAULT_GEOCODE_psv.psv",
            delimiter='|',
            schema={
                "ADDRESS_DEFAULT_GEOCODE_PID": DataType.string(),
                "DATE_CREATED": DataType.date(),
                "DATE_RETIRED": DataType.date(),
                "ADDRESS_DETAIL_PID": DataType.string(),
                "GEOCODE_TYPE_CODE": DataType.string(),
                "LONGITUDE": DataType.float64(),
                "LATITUDE": DataType.float64(),
            }
        ).with_columns(
            {
                "ADDRESS_DEFAULT_GEOCODE_CREATED": daft.col("DATE_CREATED"),
                "ADDRESS_DEFAULT_GEOCODE_RETIRED": daft.col("DATE_RETIRED"),
                "ADDRESS_DEFAULT_GEOCODE_LONGITUDE": daft.col("LONGITUDE"),
                "ADDRESS_DEFAULT_GEOCODE_LATITUDE": daft.col("LATITUDE"),
            }
        ).exclude(
            "DATE_CREATED",
            "DATE_RETIRED",
            "LONGITUDE",
            "LATITUDE",
        )
        print(f"Reading {state} info")
        df_state_state = daft.read_csv(
            path=f"data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Standard/{state}_STATE_psv.psv",
            delimiter='|',
            schema={
                "STATE_PID": DataType.string(),
                "DATE_CREATED": DataType.date(),
                "DATE_RETIRED": DataType.date(),
                "STATE_NAME": DataType.string(),
                "STATE_ABBREVIATION": DataType.string(),
            }
        ).with_columns(
            {
                "STATE_CREATED": daft.col("DATE_CREATED"),
                "STATE_RETIRED": daft.col("DATE_RETIRED"),
            }
        ).exclude(
            "DATE_CREATED",
            "DATE_RETIRED",
        )
        print(f"Joining {state} data")
        df_state_address_view = df_state_address_detail.join(
            df_state_street_locality,
            on=[daft.col("STREET_LOCALITY_PID")],
            how="left",
        ).join(
            df_state_locality,
            on=[daft.col("LOCALITY_PID")],
            how="left",
        ).join(
            df_state_address_default_geocode,
            on=[daft.col("ADDRESS_DETAIL_PID")],
            how="left",
        ).join(
            df_state_state,
            on=[daft.col("STATE_PID")],
            how="left",
        )
        if df_address_detail is None:
            df_address_detail = df_state_address_view
        else:
            df_address_detail = df_address_detail.concat(df_state_address_view)

    print("Reading flat type authority")
    df_flat_type_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_FLAT_TYPE_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "FLAT_TYPE_CODE": daft.col("CODE"),
            "FLAT_TYPE_NAME": daft.col("NAME"),
            "FLAT_TYPE_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading level type authority")
    df_level_type_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_LEVEL_TYPE_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.int8(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "LEVEL_TYPE_CODE": daft.col("CODE"),
            "LEVEL_TYPE_NAME": daft.col("NAME"),
            "LEVEL_TYPE_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading street suffix authority")
    df_street_suffix_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_STREET_SUFFIX_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "STREET_SUFFIX_CODE": daft.col("CODE"),
            "STREET_SUFFIX_NAME": daft.col("NAME"),
            "STREET_SUFFIX_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading street class authority")
    df_street_class_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_STREET_CLASS_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "STREET_CLASS_CODE": daft.col("CODE"),
            "STREET_CLASS_NAME": daft.col("NAME"),
            "STREET_CLASS_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading street type authority")
    df_street_type_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_STREET_TYPE_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "STREET_TYPE_CODE": daft.col("CODE"),
            "STREET_TYPE_NAME": daft.col("NAME"),
            "STREET_TYPE_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading geocode type authority")
    df_geocode_type_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_GEOCODE_TYPE_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "GEOCODE_TYPE_CODE": daft.col("CODE"),
            "GEOCODE_TYPE_NAME": daft.col("NAME"),
            "GEOCODE_TYPE_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Reading geocode level type authority")
    df_geocode_level_type_aut = daft.read_csv(
        path='data-sources/g-naf_nov24_allstates_gda2020_psv_1017/G-NAF/G-NAF NOVEMBER 2024/Authority Code/Authority_Code_GEOCODED_LEVEL_TYPE_AUT_psv.psv',
        delimiter='|',
        schema={
            "CODE": DataType.string(),
            "NAME": DataType.string(),
            "DESCRIPTION": DataType.string(),
        }
    ).with_columns(
        {
            "LEVEL_GEOCODED_CODE": daft.col("CODE"),
            "LEVEL_GEOCODED_NAME": daft.col("NAME"),
            "LEVEL_GEOCODED_DESCRIPTION": daft.col("DESCRIPTION"),
        }
    ).exclude("CODE", "NAME", "DESCRIPTION")

    print("Joining address view")
    address_view = df_address_detail.join(
        df_flat_type_aut,
        on=[daft.col("FLAT_TYPE_CODE")],
        how="left",
    ).join(
        df_level_type_aut,
        on=[daft.col("LEVEL_TYPE_CODE")],
        how="left",
    ).join(
        df_street_suffix_aut,
        on=[daft.col("STREET_SUFFIX_CODE")],
        how="left",
    ).join(
        df_street_class_aut,
        on=[daft.col("STREET_CLASS_CODE")],
        how="left",
    ).join(
        df_street_type_aut,
        on=[daft.col("STREET_TYPE_CODE")],
        how="left",
    ).join(
        df_geocode_type_aut,
        on=[daft.col("GEOCODE_TYPE_CODE")],
        how="left",
    ).join(
        df_geocode_level_type_aut,
        on=[daft.col("LEVEL_GEOCODED_CODE")],
        how="left",
    )
    address_view.show()

    print(f"Writing address view to delta lake directory {data_directory}")
    address_view.write_deltalake(data_directory, mode="overwrite")
    print(f"Data written to delta lake directory {data_directory}")

def get_data_directory(data_type: str):
    import os
    return os.path.join(os.getcwd(), f"data/{data_type}")

if __name__ == "__main__":
    main()
