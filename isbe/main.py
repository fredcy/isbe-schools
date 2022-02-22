import argparse
import logging
import os
import os.path
from pprint import pformat, pprint
import sqlite3
import xlrd

# About xlrd, see https://xlrd.readthedocs.io/en/latest/

# See https://www.isbe.net/Documents/key_codes.pdf for some ISBE school codes.


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("isbe")

# These are the fields from the ISBE data that we will copy over into the sqlite database.
db_fields = [
    "address",
    "city",
    "countyname",
    "facilityname",
    "gradeserved",
    "nces_id",
    "rectype",
    "rcd",
    "type",
    "school",
    "strep",
    "stsen",
    "zip",
]

rcd_colname = "Region-2\nCounty-3\nDistrict-4"


def normalize_field_name(isbe_name):
    """Convert ISBE field name (from header of Excel sheet) to name used in sqlite"""
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

    cur.execute("delete from schools")
    con.commit()

    con.close()


grade_list = [
    "P",
    "K",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "U",
]


def expand_range(range):
    """Given a string representation of a range of grades, like "K-5" or "P", return
    the equivalent set of grade values (from above `grade_list`)

    """
    grades = set()
    bounds = range.split("-")

    if len(bounds) == 1:
        # a single value, like "7"
        grades = set(bounds)

    elif len(bounds) == 2:
        # a range, like "6-8"
        first, last = bounds
        in_range = False
        for g in grade_list:
            if g == first:
                grades.add(g)
                in_range = True
            elif g == last:
                grades.add(g)
                break
            elif in_range:
                grades.add(g)
            else:
                # before first or after last
                pass

    else:
        logger.error(f"bad range: {range}")

    return grades


def expand_grades(grades_string):
    """Given a string representation of a set of grades, return the actual set of
    values.  E.g., `"1,7-9"` becomes `set(["1", "7", "8", "9"])`

    """
    grades = set()
    ranges = grades_string.split(",")
    for range in ranges:
        range_grades = expand_range(range)
        grades.update(range_grades)

    logger.debug(f"expand_grades({grades_string}) --> {grades}")
    return grades


def read_excel(args):
    book = xlrd.open_workbook(args.input)

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    field_placeholders = ", ".join(f":{n}" for n in db_fields)
    insert_stmt = f"insert into schools values ({field_placeholders})"

    sheet_count = read_count = write_count = 0

    # Those sheets and grades of interest for IMSA events, per Rich Busby
    sheets_of_interest = ["Public Dist & Sch", "Non Pub Sch"]
    grades_of_interest = expand_grades(args.grades)

    for sheet_name in book.sheet_names():
        # if not (any(s for s in sheets_of_interest if s in sheet_name)):
        #    continue

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
                continue  # empty row

            read_count += 1
            school = dict((name, row[field_index[name]]) for name in field_index)

            # Skip some non-school items
            if "Dist" in school["rectype"] or school["rectype"] in ["ROE", "ISC"]:
                continue

            # Skip schools not serving grades of interest
            grades = expand_grades(school["gradeserved"])
            if not grades.intersection(grades_of_interest):
                logger.debug(f"skipping: grades = {school['gradeserved']}")
                continue

            # Construct canonical 'address' value
            try:
                if not "address" in school:
                    school["address"] = (
                        school["delivery_address"] or school["mailing_address"]
                    )
            except KeyError:
                logger.exception(f"address problem: {school}")
                break

            if not "nces_id" in school:
                school["nces_id"] = ""

            # Insert the school data into the database
            try:
                cur.execute(insert_stmt, school)
            except:
                logger.exception(f"insert_stmt = {insert_stmt}, school = {school}")
                break

            write_count += 1

        con.commit()

    con.close()
    logger.info(
        f"Read {read_count} items from {sheet_count} sheets in {args.input} and wrote {write_count} schools to {args.db} for grades {args.grades}"
    )


def load(args):
    create_table(args)
    read_excel(args)


import urllib.request

ISBE_URL = "https://www.isbe.net/_layouts/Download.aspx?SourceUrl=/Documents/dir_ed_entities.xls"


def download(args):
    logger.debug(f"download({args})")

    dirname = os.path.dirname(args.input)
    if not os.path.exists(dirname):
        logger.info(f"creating directory {dirname}")
        os.makedirs(dirname)

    urllib.request.urlretrieve(ISBE_URL, args.input)
    logger.info(f"saved into {args.input}")


def main():
    parser = argparse.ArgumentParser(description="Read ISBE school data")
    parser.add_argument("-d", "--debug", action="store_true")
    input_default = "data/dir_ed_entities.xls"
    parser.add_argument(
        "--input",
        default=input_default,
        help=f"Excel workbook name (default: {input_default})",
    )
    db_default = "schools.db"
    parser.add_argument(
        "--db", default=db_default, help=f"Sqlite3 db file (default: {db_default})"
    )
    parser.set_defaults(func=lambda args: parser.print_help())

    subparsers = parser.add_subparsers()

    parser_download = subparsers.add_parser(
        "download", help="Download ISBE school data as Excel workbook"
    )
    parser_download.set_defaults(func=download)

    parser_load = subparsers.add_parser(
        "load", help="Load database from Excel workbook"
    )
    grades_default = "7-9"
    parser_load.add_argument(
        "--grades",
        default=grades_default,
        help=f"Grade levels of interest (default: {grades_default})",
    )
    parser_load.set_defaults(func=load)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"args = {args}")

    args.func(args)


if __name__ == "__main__":
    main()
