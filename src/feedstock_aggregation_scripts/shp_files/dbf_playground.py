import uuid

import dbf

# def copy_dbf_on_disk(path_to_data):

#     dbf.op
#     # make a copy of the test table (structure, not data)
#     custom = dbf.Table.new(
#         filename="test_on_disk.dbf",
#         default_data_types={"C": dbf.Char, "D": dbf.Date, "L": dbf.Logical},
#     )

#     # automatically opened and closed
#     with custom:
#         # copy records from test to custom
#         for record in table:
#             custom.append(record)
#         # modify each record in custom (could have done this in prior step)
#         for record in custom:
#             dbf.write(record, name=record.name.upper())
#             # and print the modified record
#             print(record)
#             print("--------")
#             print(record[0:3])
#             print([record.name, record.age, record.birth])
#             print("--------")


def run3():
    table = dbf.Table("temptable", "name C(30); age N(3,0); birth D")

    print("db definition created with field names:", table.field_names)

    table.open(mode=dbf.READ_WRITE)
    for datum in (
        ("John Doe", 31, dbf.Date(1979, 9, 13)),
        ("Ethan Furman", 102, dbf.Date(1909, 4, 1)),
        ("Jane Smith", 57, dbf.Date(1954, 7, 2)),
        ("John Adams", 44, dbf.Date(1967, 1, 9)),
    ):
        table.append(datum)

    print("records added:")
    for record in table:
        print(record)
        print("-----")

    table.close()

    table.open(mode=dbf.READ_WRITE)

    table.add_fields("telephone C(10)")

    names = {
        "John Doe": "Mom and Dads",
        "Ethan Furman": "2345",
        "Jane Smith": "3456",
        "John Adams": "4567",
        "Mom and Dads": "112",
    }
    for record in table:
        with record as r:
            r.name = names[r.name.strip()]
            # r.telephone = telephones[r.name.strip()]

    print("updated records")
    for record in table:
        print(record)
        print("-----")


def adjust_dbfs(filenames, grower, change_dict, key_dict):
    for filename in filenames:
        table = dbf.Table(filename=filename)
        print("db definition read with field names:", table.field_names)

        table.open(mode=dbf.READ_WRITE)
        if "FIELD_NAME" not in table.field_names:
            table.add_fields("FIELD_NAME C(254)")

        if "FARM_NAME" not in table.field_names:
            table.add_fields("FARM_NAME C(254)")

        if "CLIENT_NAM" not in table.field_names:
            table.add_fields("CLIENT_NAM C(254)")

        if "FIELD_ID" not in table.field_names:
            table.add_fields("FIELD_ID C(254)")

        if "FARM_ID" not in table.field_names:
            table.add_fields("FARM_ID C(254)")

        if "CLIENT_ID" not in table.field_names:
            table.add_fields("CLIENT_ID C(254)")

        for record in table:
            with record as r:
                replace_val = change_dict.get(r.FIELD.strip(), False)
                r["FIELD_NAME"] = replace_val if replace_val else r.FIELD.strip()

                r["FARM_NAME"] = r.FARM.strip()
                r["CLIENT_NAM"] = r.GROWER.strip()
                # set ids
                r["FIELD_ID"] = uuid.uuid4().__str__()
                r["FARM_ID"] = key_dict["FARMS"].get(r.FARM.strip(), None)
                r["CLIENT_ID"] = key_dict["CLIENT"].get(grower, None)

        if "FIELD" in table.field_names:
            table.delete_fields("FIELD")

        if "FARM" in table.field_names:
            table.delete_fields("FARM")

        print("updated records")
        for record in table:
            print(record)
            print("-----")

    return True


def run2():
    print("running...")
    table = dbf.Table(
        filename="/workspaces/feedstock-aggregation-scripts/data/01_data/Field/shp-files/FjeldeHomeSouth_FjeldeFarms_MATTFIELD_2023-03-27-27-49/FjeldeHomeSouth_FjeldeFarms_MATTFIELD_2023-03-27-27-49.dbf",
        # field_specs="name C(25); age N(3,0); birth D; qualified L",
        # on_disk=False,
    )
    table.open(dbf.READ_WRITE)
    print(table)
    for record in table:
        # with record as r:
        #     print(r)
        #     if r == "FIELD_NAME":
        #         print(r)
        print(record)
        print("--------")


def run():
    # create an in-memory table
    table = dbf.Table(
        filename="/workspaces/feedstock-aggregation-scripts/data/01_data/Field/shp-files/FjeldeHomeSouth_FjeldeFarms_MATTFIELD_2023-03-27-27-49/FjeldeHomeSouth_FjeldeFarms_MATTFIELD_2023-03-27-27-49.dbf",
        # field_specs="name C(25); age N(3,0); birth D; qualified L",
        # on_disk=False,
    )
    table.open(dbf.READ_WRITE)

    # add some records to it
    # for datum in (
    #     ("Spanky", 7, dbf.Date.fromymd("20010315"), False),
    #     ("Spunky", 23, dbf.Date(1989, 7, 23), True),
    #     ("Sparky", 99, dbf.Date(), dbf.Unknown),
    # ):
    #     table.append(datum)
    # table.add_fields("FIELD_NAME C(20)")

    # dbf.write(record="FIELD_NAME", data="Mom and Dads")

    # iterate over the table, and print the records
    for record in table:
        # with record as r:
        #     print(r)
        #     if r == "FIELD_NAME":
        #         print(r)
        print(record)
        print("--------")
        # print(record[0:3])
        # # print([record.name, record.age, record.birth])
        # print("--------")

    # make a copy of the test table (structure, not data)
    table.new(
        filename="test_on_disk.dbf",
        default_data_types={"C": dbf.Char, "D": dbf.Date, "L": dbf.Logical},
    )

    # automatically opened and closed
    # with custom:
    #     # copy records from test to custom
    #     for record in table:
    #         custom.append(record)
    #     # modify each record in custom (could have done this in prior step)
    #     for record in custom:
    #         dbf.write(record, field=record.FIELD.upper())
    #         # and print the modified record
    #         print(record)
    #         print("--------")
    #         print(record[0:3])
    #         # print([record.name, record.age, record.birth])
    #         print("--------")

    table.close()
    table.close()
