import argparse
import logging
from pprint import pformat, pprint
import sqlite3
import xlrd

# About xlrd, see https://xlrd.readthedocs.io/en/latest/

# See https://www.isbe.net/Documents/key_codes.pdf for some ISBE school codes.


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("isbe")

# These are the fields from the ISBE data that we will copy over into the sqlite database.
db_fields = ["address", "city", "countyname", "facilityname", "rectype", "rcd", "type", "school", "zip"]

rcd_colname = 'Region-2\nCounty-3\nDistrict-4'

def normalize_field_name(isbe_name):
    """ Convert ISBE field name (from header of Excel sheet) to name used in sqlite"""
    if isbe_name == rcd_colname:
        return "rcd"
    else:
        return isbe_name.lower().replace(" ", "_")


def create_table(args):
    field_list = ", ".join(f"{name} text" for name in db_fields)
    q = f"create table if not exists schools ( {field_list} )"
    logger.debug(f"{q}")
    
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(q)
    con.commit()
    con.close()


def read_excel(args):
    book = xlrd.open_workbook(args.input)

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    field_placeholders = ", ".join(f":{n}" for n in db_fields)
    insert_stmt = f"insert into schools values ({field_placeholders})"

    sheet_count = read_count = write_count = 0

    for sheet_name in book.sheet_names():
        logger.debug(f"sheet: {sheet_name}")
        sheet = book.sheet_by_name(sheet_name)

        header_row = sheet.row_values(0)
        if not rcd_colname in header_row:
            logger.debug(f"skipping sheet {sheet_name}")
            continue

        sheet_count += 1

        # Get the field names from the header row, converting them to names used in this app
        fields = [normalize_field_name(f) for f in header_row]

        field_index = dict(zip(fields, range(len(fields))))
        
        for rownum in range(1, min(sheet.nrows, 5000000)):
            row = sheet.row_values(rownum)
            if not row[field_index["rcd"]]:
                continue        # empty row

            read_count += 1
            school = dict((name, row[field_index[name]]) for name in field_index)

            # Skip some non-school items
            if "Dist" in school["rectype"] or school["rectype"] in ["ROE", "ISC"]:
                continue

            #logger.debug(f"{pformat(school)}")

            try:
                if not "address" in school:
                    school["address"] = school["delivery_address"] or school["mailing_address"]
            except KeyError:
                logger.exception(f"address problem: {school}")
                break

            try:
                cur.execute(insert_stmt, school)
            except:
                logger.exception(f"insert_stmt = {insert_stmt}, school = {school}")
                break
                
            write_count += 1

        con.commit()

    con.close()
    logger.info(f"Read {read_count} items from {sheet_count} sheets and wrote {write_count} schools")


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
    
