import argparse
import logging
from pprint import pformat, pprint
import sqlite3
import xlrd

# About xlrd, see https://xlrd.readthedocs.io/en/latest/


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("isbe")

db_fields = ["city", "countyname", "facilityname"]

def create_table(args):
    field_list = ", ".join(f"{name} text" for name in db_fields)
    q = f"create table if not exists schools ( {field_list} )"
    logger.debug(f"{q}")
    
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(q)
    con.commit()
    con.close()


rcd_colname = 'Region-2\nCounty-3\nDistrict-4'

def read_excel(args):
    book = xlrd.open_workbook(args.input)

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    field_placeholders = ", ".join(f":{n}" for n in db_fields)
    insert_stmt = f"insert into schools values ({field_placeholders})"
    logger.debug(f"{insert_stmt}")

    for sheet_name in book.sheet_names():
        logger.debug(f"sheet: {sheet_name}")
        sheet = book.sheet_by_name(sheet_name)

        header_row = sheet.row_values(0)
        if not rcd_colname in header_row:
            logger.debug(f"skipping sheet {sheet_name}")
            continue

        fields = [("rcd" if f == rcd_colname else f.lower().replace(" ", "_")) for f in header_row]
        field_index = dict(zip(fields, range(len(fields))))
        
        for rownum in range(1, min(sheet.nrows, 5000000)):
            row = sheet.row_values(rownum)
            if not row[field_index["rcd"]]:
                continue        # empty row
            
            school = dict((field_name, row[field_index[field_name]]) for field_name in field_index)
            logger.debug(f"{pformat(school)}")

            cur.execute(insert_stmt, school)

        con.commit()

    con.close()

def main():
    parser = argparse.ArgumentParser(description="Read ISBE school data")
    parser.add_argument("-d", "--debug", action="store_true")
    input_default = "data/dir_ed_entities.xls"
    parser.add_argument("--input", default=input_default, help="Excel workbook name")
    db_default = "schools.db"
    parser.add_argument("--db", default=db_default, help="Sqlite3 db file")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"args = {args}")

    create_table(args)
    read_excel(args)


if __name__ == "__main__":
    main()
    
