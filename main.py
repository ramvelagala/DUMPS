SET @rowindex := -1;
 
SELECT
   AVG(g.grade)
FROM
   (SELECT @rowindex:=@rowindex + 1 AS rowindex,
           grades.grade AS grade
    FROM grades
    ORDER BY grades.grade) AS g
WHERE
g.rowindex IN (FLOOR(@rowindex / 2) , CEIL(@rowindex / 2));
