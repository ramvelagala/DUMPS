DROP TEMPORARY TABLE IF EXIST temp_table;
CREATE TEMPORARY TABLE temp_table(SELECT * FROM $table LIMIT 0);

PREPARE STMT FROM 'INSERT INTO temp_table SELECT * FROM $table LIMIT ?, 1';
DELIMITER $$
DROP PROCEDURE IF EXISTS my_proc$$
CREATE PROCEDURE my_proc(input INT)
BEGIN
    SET @shift = 0;
    SET @max = 0;
    SELECT COUNT(*) FROM $table INTO @max;

    my_loop: LOOP
        IF (input = 0) THEN
            LEAVE my_loop;
        END IF;
        SET input = input - 1;
		string = concat(string_value,x

        SET @shift = CAST(RAND()*@max AS UNSIGNED);
        EXECUTE STMT USING @shift;

    END LOOP;
    SELECT * FROM temp_table;
END $$
DELIMITER;

CALL my_proc($row_count)
