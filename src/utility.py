import csv

def error_print(condition, error_string):
    """ Printing error if condition is true"""
    if condition:
        print("ERROR : {}".format(error_string))
        exit(-1)

def isint(s):
    try:
        _ = int(s)
        return True
    except:
        return False

def get_relate_op(cond):
    if "<=" in cond:
        op = "<="
    elif ">=" in cond: 
        op = ">="
    elif "<>" in cond: 
        op = "<>"
    elif ">" in cond: 
        op = ">"
    elif "<" in cond: 
        op = "<"
    elif "=" in cond: 
        op = "="
    else : 
        error_print(True, "invalid condition : '{}'".format(cond))

    error_print(cond.count(op) != 1, "invalid condition : '{}'".format(cond))
    l, r = cond.split(op)
    l = l.strip()
    r = r.strip()
    return op, l, r

def load_table(fname):
    ll = list(csv.reader(open(fname, "r")))
    return ( list(map(lambda x : list(map(int, x)), ll)))

def print_table(header, table,dp):
    if not dp:
        print(",".join(map(str, header)))
        for row in table:
            print(",".join(map(str, row)))
    else:
        index = -1
        for i,x in enumerate(header):
            if(x.split('.')[0] == dp[0] and x.split('.')[1] == dp[1]):
                index = i
        print(index)
        print(",".join(map(str, header[:index])),end='')
        print(",",end='')
        print(",".join(map(str, header[index+1:])),end='')
        print()
        for row in table:
            print(",".join(map(str, row[:index])),end='')
            print(",",end='')
            print(",".join(map(str, row[index+1:])),end='')
            print()
        


def break_query(q):
    """ Check the structure of select, from and where and return raw separated query to be parsed further"""
    q = q.split(';')[0]    
    toks = q.lower().split()
    error_print(toks[0]!="select","Only select command is allowed")

    # getting the indices of select, from, where
    select_idx = [idx for idx, t in enumerate(toks) if t == "select"]
    from_idx = [idx for idx, t in enumerate(toks) if t == "from"]
    where_idx = [idx for idx, t in enumerate(toks) if t == "where"]

    # error handling for improper number of arguments
    error_print((len(select_idx) != 1) or (len(from_idx) != 1) or (len(where_idx) > 1), "invalid query")
    select_idx, from_idx = select_idx[0], from_idx[0]
    where_idx = where_idx[0] if len(where_idx) == 1 else None
    error_print(from_idx <= select_idx, "invalid query")
    if where_idx: 
        error_print(where_idx <= from_idx, "invalid query")

    raw_cols = toks[select_idx+1:from_idx]
    if where_idx:
        raw_tables = toks[from_idx+1:where_idx]
        raw_condition = toks[where_idx+1:]
    else:
        raw_tables = toks[from_idx+1:]
        raw_condition = []

    error_print(len(raw_tables) == 0, "no tables after 'from'")
    error_print(where_idx != None and len(raw_condition) == 0, "no conditions after 'where'")
    
    return raw_tables, raw_cols, raw_condition
