select concat(' SELECT * FROM t WHERE ''a'' in ('
             , GROUP_CONCAT(COLUMN_NAME)
             , ')')
from INFORMATION_SCHEMA.columns 
where table_schema = 's' 
  and table_name = 't'
  and DATA_TYPE IN ('char','varchar');
