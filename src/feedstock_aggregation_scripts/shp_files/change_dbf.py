import uuid

import dbf


def adjust_dbfs(
    filenames: list[str], grower: str, change_dict: dict, key_dict: dict
) -> None:
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
                r["FIELD_ID"] = uuid.uuid4().__str__()  # individual id for each field
                r["FARM_ID"] = key_dict["FARMS"].get(
                    r.FARM.strip(), None
                )  # id depends on farm
                r["CLIENT_ID"] = key_dict["CLIENT"].get(
                    grower, None
                )  # same id per client

        if "FIELD" in table.field_names:
            table.delete_fields("FIELD")

        if "FARM" in table.field_names:
            table.delete_fields("FARM")

        print("updated records")
        for record in table:
            print(record)
            print("-----")
