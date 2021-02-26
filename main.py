# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

SELECT  
 
   attname       AS COL_NAME
FROM _v_table a
   JOIN _v_relation_column b
   ON a.objid   = b.objid
WHERE a.tablename = 'M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL'
AND COL_NAME LIKE '%_SK'
AND a.schema = 'ADMIN'
ORDER BY attnum;  
 
 
2)
Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = " " and TABLE_SCHEMA = " "
