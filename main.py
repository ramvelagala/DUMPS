SELECT DISTINCT AGILEGROUP, COUNT(AGILEGROUP) as frequ
FROM DBKDMDB.ADMIN.AGILELABBERS p1
WHERE ((SELECT  count(*) FROM DBKDMDB.ADMIN.AGILELABBERS p2
WHERE p2.AGILEGROUP >= p1.AGILEGROUP ) <=
(SELECT  0.25 * count(*) FROM DBKDMDB.ADMIN.AGILELABBERS) )
GROUP BY p1.AGILEGROUP
DESC
;





SELECT  count($column) as minus_count, count(*) as count1, minus_count|count1 as percent
 FROM $table
Where  $column = -1
