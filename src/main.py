import sys
import re
import itertools
import os

from utility import error_print,isint,get_relate_op,load_table,print_table,break_query

DB_DIR = "../files/"
META_FILE = "../files/metadata.txt"
LITERAL = "<literal>"
schema = {}
all_star = False

def init_metadata():
    """ Read the specified metadata file and get schema of tables"""
    table_name = None
    with open(META_FILE, "r") as metafile:
        contents = metafile.readlines()
    contents = [t.strip() for t in contents if t.strip()]
    for t in contents:
        t = t.lower()
        if t == "<begin_table>": 
            _, table_name = [], None
        elif t == "<end_table>": 
            pass
        elif not table_name: 
            table_name, schema[t] = t, []
        else:
            schema[table_name].append(t)

def get_output_table(qdict):
    """ load all tables and retain only necessary columns
    also decide the indices of intermediate table columns"""
    colidx = {}
    cnt = 0
    all_tables = []

    alias2tb = qdict['alias2tb']
    inter_cols = qdict['inter_cols']
    tables = qdict['tables']
    conditions = qdict['conditions']
    cond_op = qdict['cond_op']
    proj_cols = qdict['proj_cols']
    dont_print = []
    
    for t in tables:
        lt = load_table(os.path.join(DB_DIR, "{}.csv".format( alias2tb[t] )))
        idxs = [schema[alias2tb[t]].index(cname) for cname in inter_cols[t]]
        
        output_table = []
        for row in lt:
            row_table = []
            for x in idxs:
                row_table.append(row[x])
            output_table.append(row_table)    
        all_tables.append(output_table)

        colidx[t] = {cname: cnt+i for i, cname in enumerate(inter_cols[t])}
        cnt += len(inter_cols[t])

    # cartesian product of all tables

    inter_table = [[i for tup in r for i in list(tup)] for r in itertools.product(*all_tables)]

    # check for conditions and get reduced table

    if len(conditions):
        cols_to_take = []
        for i in range(len(inter_table)):
            ap = []
            for j in range(len(conditions)):
                ap.append(True)
            cols_to_take.append(ap)    
        
        if all_star==True:
            for i,c in enumerate(conditions):
                if(c[0]=='=' and c[2][0]!=LITERAL):
                    dont_print.append(c[2][0])
                    dont_print.append(c[2][1])

        # Apply all aggregate functions

        for idx, (op, left, right) in enumerate(conditions):
            cols = []
            for tname, cname in [left, right]:
                if tname == LITERAL:
                    ap = []
                    for i in range(len(inter_table)):
                        ap.append(int(cname)) 
                    cols.append(ap)
                else:
                    output = []
                    for row in inter_table:
                        output.append(row[colidx[tname][cname]])
                    cols.append(output)
            if op=="<=":
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] <= cols[1][i])
            if op==">=": 
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] >= cols[1][i])
            if op=="<>": 
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] != cols[1][i])
            if op=="<":
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] < cols[1][i])
            if op==">": 
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] > cols[1][i])
            if op=="=": 
                for i in range(len(cols_to_take)):
                    cols_to_take[i][idx] = (cols[0][i] == cols[1][i])
        if cond_op == " or ":
            final_take = [] 
            for i in range(len(cols_to_take)):
                final_take.append(cols_to_take[i][0]|cols_to_take[i][1])
        elif cond_op == " and ":
            final_take = [] 
            for i in range(len(cols_to_take)):
                final_take.append(cols_to_take[i][0]&cols_to_take[i][1])
        else:
            final_take = [] 
            for i in range(len(cols_to_take)):
                final_take.append(cols_to_take[i][0])
        op = []
        for i in range(len(inter_table)):
            if final_take[i] == True:
                op.append(inter_table[i])
        inter_table = op    

    select_idxs = [colidx[tn][cn] for tn, cn, aggr in proj_cols]
    output_table = []
    
    for row in inter_table:
        row_table = []
        for x in select_idxs:
            row_table.append(row[x])
        output_table.append(row_table)  
    inter_table = output_table
    
    # process for aggregate function
    
    if proj_cols[0][2]:
        out_table = []
        disti = False
        for idx, (_, _, aggr) in enumerate(proj_cols):
            col = []
            for r in inter_table:
                col.append(r[idx])
            if not col:
                error_print(True,"no columns to display")
            else:
                if aggr == "min": 
                    out_table.append(min(col))
                elif aggr == "max": 
                    out_table.append(max(col))
                elif aggr == "sum": 
                    out_table.append(sum(col))
                elif aggr == "average": 
                    out_table.append(sum(col)/len(col))
                elif aggr == "distinct":
                    disti = True
                else: 
                    error_print(True, "invalid aggregate")
        
        # putting output table as a list of lists
        out_table = [out_table]
    
        # changes to header and table when distinct/ aggregate funcitons have been applied
        if disti:
            out_table = [list(x) for x in set(tuple(x) for x in inter_table)]
            out_header = ["{}.{}".format(tn, cn) for tn, cn, _ in proj_cols]
            out_header[0]  = "distinct(" + out_header[0]
            out_header[-1] = out_header[-1]+")"
        else:
            out_header = ["{}({}.{})".format(aggr,tn, cn) for tn, cn, aggr in proj_cols]
    
    # generate header and output table when no aggregate functions
    else:
        out_table = inter_table
        out_header = ["{}.{}".format(tn, cn) for tn, cn, aggr in proj_cols]

    return out_header, out_table, dont_print

def parse_tables(raw_tables):
    """ all joined tables """
    
    raw_tables = " ".join(raw_tables).split(",")
    
    # stores all table names
    tables = []
    
    # stores all table aliases with the actual table names
    alias2tb = {}
    
    for rt in raw_tables:
        t = rt.split()
        error_print(not(len(t) == 1 or (len(t) == 3 and t[1] == "as")), "invalid table specification '{}'".format(rt))
        if len(t) == 1: 
            tb_name, tb_alias = t[0], t[0]
        else: 
            tb_name, _, tb_alias = t
    
        # raise error if there was no such table in the schema provided
        error_print(tb_name not in schema.keys(), "no table named '{}'".format(tb_name))
    
        # raise error if the alias used for the current table isn't unique
        error_print(tb_alias in alias2tb.keys(), "not a unique table alias '{}'".format(tb_alias))

        tables.append(tb_alias)
        alias2tb[tb_alias] = tb_name
 
    return tables, alias2tb

def parse_proj_cols(raw_cols, tables, alias2tb):
    """ projection columns are the columns to output """
    disti = False
    
    # handling distinct tag
    if(raw_cols[0] == 'distinct'):
        disti = True
        raw_cols = raw_cols[1:]

    raw_cols = "".join(raw_cols).split(",")
    proj_cols = []
 
    for rc in raw_cols:
        # match for aggregate function
        regmatch = re.match(r"(.+)\((.+)\)", rc)
        if regmatch: 
            aggr, rc = regmatch.groups()
        else: 
            aggr = None
 
        # format is either one of these two : col or table.col
        error_print("." in rc and len(rc.split(".")) != 2, "invalid column name '{}'".format(rc))

        # See if the aggregrate function is distinct(different format)
        if(disti==True):
            aggr = 'distinct'
 
        # get table name and column name
        tname = None
        if "." in rc:
            tname, cname = rc.split(".")
            error_print(tname not in alias2tb.keys(), "unknown field : '{}'".format(rc))
        else:
            cname = rc
            if cname != "*":
                tname = [t for t in tables if cname in schema[alias2tb[t]]]
                error_print(len(tname) > 1, "not unique field : '{}'".format(rc))
                error_print(len(tname) == 0, "unknown field : '{}'".format(rc))
                tname = tname[0]

        # add all columns if *
        if cname == "*":
            error_print(aggr != None, "can't use aggregate '{}'".format(aggr))
            all_star = True
            if tname != None:
                proj_cols.extend([(tname, c, aggr) for c in schema[alias2tb[tname]]])
            else:
                for t in tables:
                    proj_cols.extend([(t, c, aggr) for c in schema[alias2tb[t]]])
        else:
            error_print(cname not in schema[alias2tb[tname]], "unknown field : '{}'".format(rc))
            proj_cols.append((tname, cname, aggr))

    # either all columns without aggregate or all columns with aggregate
    s = [a for t, c, a in proj_cols]
    error_print(all(s) ^ any(s), "aggregated and nonaggregated columns are not allowed simultaneously")

    return proj_cols

def parse_conditions(raw_condition, tables, alias2tb):
    """ function to parse conditions provided by user and return conditions and operators associated"""
    conditions = []
    cond_op = None
    
    if raw_condition:
        
        raw_condition = " ".join(raw_condition)
        # handling OR and AND conditions
        if " or " in raw_condition: 
            cond_op = " or "
        elif " and " in raw_condition: 
            cond_op = " and "

        if cond_op: 
            raw_condition = raw_condition.split(cond_op)
        else: 
            raw_condition = [raw_condition]

        for cond in raw_condition:
            relate_op, left, right = get_relate_op(cond)
            parsed_cond = [relate_op]
            for _, rc in enumerate([left, right]):
                if isint(rc):
                    parsed_cond.append((LITERAL, rc))
                    continue

                if "." in rc:
                    tname, cname = rc.split(".")
                else:
                    cname = rc
                    tname = [t for t in tables if rc in schema[alias2tb[t]]]
                    error_print(len(tname) > 1, "not unique field : '{}'".format(rc))
                    error_print(len(tname) == 0, "unknown field : '{}'".format(rc))
                    tname = tname[0]
                error_print((tname not in alias2tb.keys()) or (cname not in schema[alias2tb[tname]]),
                    "unknown field : '{}'".format(rc))
                parsed_cond.append((tname, cname))
            conditions.append(parsed_cond)

    return conditions, cond_op


def parse_query(q):
    """ Parse the raw query provided and process sequentially"""
    # break the query
    raw_tables, raw_cols, raw_condition = break_query(q)
    # get the tables
    tables, alias2tb = parse_tables(raw_tables)
    # get the columns to be projected
    proj_cols = parse_proj_cols(raw_cols, tables, alias2tb)
    # get the conditions to project the columns under
    conditions, cond_op = parse_conditions(raw_condition, tables, alias2tb)
    # decide the needed columns for each table
    inter_cols = {t : set() for t in tables}
    
    for tn, cn, _ in proj_cols: 
        inter_cols[tn].add(cn)
    
    for cond in conditions:
        for tn, cn in cond[1:]:
            if tn == LITERAL:
                continue
            inter_cols[tn].add(cn)

    for t in tables: 
        inter_cols[t] = list(inter_cols[t])
    
    return {
        'tables':tables,
        'alias2tb':alias2tb,
        'proj_cols':proj_cols,
        'conditions':conditions,
        'cond_op':cond_op,
        'inter_cols':inter_cols,
    }

def main():
    init_metadata()
    # ensuring command is of format "python3 <file.py> <command>"
    if len(sys.argv) != 2:
        print("ERROR : invalid number of arguments")
        print("USAGE : python {} '<sql query>'".format(sys.argv[0]))
        exit(-1)
    query = sys.argv[1]
    # parsing the query and getting a dictionary of seperate options
    qdict = parse_query(query)
    # getting the output header, output table and a dont_print flag for join operation
    out_header, out_table , dp = get_output_table(qdict) 
    # printing the output
    print_table(out_header, out_table,dp)


if __name__ == "__main__":
    main()
