DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `insert_23dsGroup_Data`()
BEGIN
    DECLARE done TINYINT DEFAULT 0;
    DECLARE v_group_id INT;

    DECLARE v_nfr_list      TEXT;
    DECLARE v_source_list   TEXT;
    DECLARE v_activity_list TEXT;
    DECLARE v_json          TEXT;

    DECLARE cur_groups CURSOR FOR
        SELECT id FROM NAEI_global.t_Group;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- scratch buffer that mirrors the shape of t_Group_Data (minus the id column)
    DROP TEMPORARY TABLE IF EXISTS tmp_group_buffer;
    CREATE TEMPORARY TABLE tmp_group_buffer (
        Group_id     INT UNSIGNED NOT NULL,
        Pollutant_id INT UNSIGNED NOT NULL,
        f1970 DECIMAL(32,20) DEFAULT NULL,
        f1971 DECIMAL(32,20) DEFAULT NULL,
        f1972 DECIMAL(32,20) DEFAULT NULL,
        f1973 DECIMAL(32,20) DEFAULT NULL,
        f1974 DECIMAL(32,20) DEFAULT NULL,
        f1975 DECIMAL(32,20) DEFAULT NULL,
        f1976 DECIMAL(32,20) DEFAULT NULL,
        f1977 DECIMAL(32,20) DEFAULT NULL,
        f1978 DECIMAL(32,20) DEFAULT NULL,
        f1979 DECIMAL(32,20) DEFAULT NULL,
        f1980 DECIMAL(32,20) DEFAULT NULL,
        f1981 DECIMAL(32,20) DEFAULT NULL,
        f1982 DECIMAL(32,20) DEFAULT NULL,
        f1983 DECIMAL(32,20) DEFAULT NULL,
        f1984 DECIMAL(32,20) DEFAULT NULL,
        f1985 DECIMAL(32,20) DEFAULT NULL,
        f1986 DECIMAL(32,20) DEFAULT NULL,
        f1987 DECIMAL(32,20) DEFAULT NULL,
        f1988 DECIMAL(32,20) DEFAULT NULL,
        f1989 DECIMAL(32,20) DEFAULT NULL,
        f1990 DECIMAL(32,20) DEFAULT NULL,
        f1991 DECIMAL(32,20) DEFAULT NULL,
        f1992 DECIMAL(32,20) DEFAULT NULL,
        f1993 DECIMAL(32,20) DEFAULT NULL,
        f1994 DECIMAL(32,20) DEFAULT NULL,
        f1995 DECIMAL(32,20) DEFAULT NULL,
        f1996 DECIMAL(32,20) DEFAULT NULL,
        f1997 DECIMAL(32,20) DEFAULT NULL,
        f1998 DECIMAL(32,20) DEFAULT NULL,
        f1999 DECIMAL(32,20) DEFAULT NULL,
        f2000 DECIMAL(32,20) DEFAULT NULL,
        f2001 DECIMAL(32,20) DEFAULT NULL,
        f2002 DECIMAL(32,20) DEFAULT NULL,
        f2003 DECIMAL(32,20) DEFAULT NULL,
        f2004 DECIMAL(32,20) DEFAULT NULL,
        f2005 DECIMAL(32,20) DEFAULT NULL,
        f2006 DECIMAL(32,20) DEFAULT NULL,
        f2007 DECIMAL(32,20) DEFAULT NULL,
        f2008 DECIMAL(32,20) DEFAULT NULL,
        f2009 DECIMAL(32,20) DEFAULT NULL,
        f2010 DECIMAL(32,20) DEFAULT NULL,
        f2011 DECIMAL(32,20) DEFAULT NULL,
        f2012 DECIMAL(32,20) DEFAULT NULL,
        f2013 DECIMAL(32,20) DEFAULT NULL,
        f2014 DECIMAL(32,20) DEFAULT NULL,
        f2015 DECIMAL(32,20) DEFAULT NULL,
        f2016 DECIMAL(32,20) DEFAULT NULL,
        f2017 DECIMAL(32,20) DEFAULT NULL,
        f2018 DECIMAL(32,20) DEFAULT NULL,
        f2019 DECIMAL(32,20) DEFAULT NULL,
        f2020 DECIMAL(32,20) DEFAULT NULL,
        f2021 DECIMAL(32,20) DEFAULT NULL,
        f2022 DECIMAL(32,20) DEFAULT NULL,
        f2023 DECIMAL(32,20) DEFAULT NULL,
        PRIMARY KEY (Group_id, Pollutant_id)
    ) ENGINE = MEMORY;

    DELETE FROM t_Group_Data;
    ALTER TABLE t_Group_Data AUTO_INCREMENT = 1;

    OPEN cur_groups;

    read_loop: LOOP
        FETCH cur_groups INTO v_group_id;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET v_nfr_list      = (SELECT NFRCode     FROM NAEI_global.t_Group WHERE id = v_group_id);
        SET v_source_list   = (SELECT SourceName  FROM NAEI_global.t_Group WHERE id = v_group_id);
        SET v_activity_list = (SELECT ActivityName FROM NAEI_global.t_Group WHERE id = v_group_id);

        DROP TEMPORARY TABLE IF EXISTS tmp_nfrcode_ids;
        DROP TEMPORARY TABLE IF EXISTS tmp_source_ids;
        DROP TEMPORARY TABLE IF EXISTS tmp_activity_ids;

	    CREATE TEMPORARY TABLE tmp_nfrcode_ids (
	        NFRCode_id INT UNSIGNED NOT NULL,
	        PRIMARY KEY (NFRCode_id)
	    ) ENGINE = MEMORY;

	    IF v_nfr_list IS NULL OR v_nfr_list = '' THEN
	        INSERT IGNORE INTO tmp_nfrcode_ids
	        SELECT DISTINCT NFRCode_id
	        FROM NAEI2023ds.t_Data;
	    ELSE
	        SET v_json = CONCAT('["', REPLACE(v_nfr_list, '; ', '","'), '"]');
	        INSERT IGNORE INTO tmp_nfrcode_ids
	        SELECT DISTINCT nc.id
	        FROM JSON_TABLE(v_json, '$[*]' COLUMNS (code VARCHAR(255) PATH '$')) jt
	        JOIN NAEI_global.t_NFRCode nc
	          ON nc.NFRCode = jt.code;
	    END IF;

        CREATE TEMPORARY TABLE tmp_source_ids (
            SourceName_id INT UNSIGNED NOT NULL,
            PRIMARY KEY (SourceName_id)
        ) ENGINE = MEMORY;

		IF v_source_list IS NULL OR v_source_list = '' THEN
		    INSERT IGNORE INTO tmp_source_ids
		    SELECT DISTINCT td.SourceName_id
		    FROM NAEI2023ds.t_Data td
		    WHERE v_group_id = 1
		       OR td.NFRCode_id IN (SELECT NFRCode_id FROM tmp_nfrcode_ids);
		ELSE
		    SET v_json = CONCAT('["', REPLACE(v_source_list, '; ', '","'), '"]');
		    INSERT IGNORE INTO tmp_source_ids
		    SELECT DISTINCT sn.id
		    FROM JSON_TABLE(v_json, '$[*]' COLUMNS (name VARCHAR(255) PATH '$')) jt
		    JOIN NAEI_global.t_SourceName sn ON sn.SourceName = jt.name;
		END IF;


        CREATE TEMPORARY TABLE tmp_activity_ids (
            ActivityName_id INT UNSIGNED NOT NULL,
            PRIMARY KEY (ActivityName_id)
        ) ENGINE = MEMORY;


		IF v_activity_list IS NULL OR v_activity_list = '' THEN
		    INSERT IGNORE INTO tmp_activity_ids
		    SELECT DISTINCT td.ActivityName_id
		    FROM NAEI2023ds.t_Data td
		    WHERE v_group_id = 1
		       OR td.SourceName_id IN (SELECT SourceName_id FROM tmp_source_ids);
		ELSE
		    SET v_json = CONCAT('["', REPLACE(v_activity_list, '; ', '","'), '"]');
		    INSERT IGNORE INTO tmp_activity_ids
		    SELECT DISTINCT an.id
		    FROM JSON_TABLE(v_json, '$[*]' COLUMNS (name VARCHAR(255) PATH '$')) jt
		    JOIN NAEI_global.t_ActivityName an ON an.ActivityName = jt.name;
		END IF;

        INSERT INTO tmp_group_buffer (
            Group_id,
            Pollutant_id,
            f1970, f1971, f1972, f1973, f1974, f1975, f1976, f1977, f1978, f1979,
            f1980, f1981, f1982, f1983, f1984, f1985, f1986, f1987, f1988, f1989,
            f1990, f1991, f1992, f1993, f1994, f1995, f1996, f1997, f1998, f1999,
            f2000, f2001, f2002, f2003, f2004, f2005, f2006, f2007, f2008, f2009,
            f2010, f2011, f2012, f2013, f2014, f2015, f2016, f2017, f2018, f2019,
            f2020, f2021, f2022, f2023
        )
        SELECT
            v_group_id,
            td.Pollutant_id,
            SUM(td.f1970), SUM(td.f1971), SUM(td.f1972), SUM(td.f1973), SUM(td.f1974), SUM(td.f1975),
            SUM(td.f1976), SUM(td.f1977), SUM(td.f1978), SUM(td.f1979),
            SUM(td.f1980), SUM(td.f1981), SUM(td.f1982), SUM(td.f1983), SUM(td.f1984), SUM(td.f1985),
            SUM(td.f1986), SUM(td.f1987), SUM(td.f1988), SUM(td.f1989),
            SUM(td.f1990), SUM(td.f1991), SUM(td.f1992), SUM(td.f1993), SUM(td.f1994), SUM(td.f1995),
            SUM(td.f1996), SUM(td.f1997), SUM(td.f1998), SUM(td.f1999),
            SUM(td.f2000), SUM(td.f2001), SUM(td.f2002), SUM(td.f2003), SUM(td.f2004), SUM(td.f2005),
            SUM(td.f2006), SUM(td.f2007), SUM(td.f2008), SUM(td.f2009),
            SUM(td.f2010), SUM(td.f2011), SUM(td.f2012), SUM(td.f2013), SUM(td.f2014), SUM(td.f2015),
            SUM(td.f2016), SUM(td.f2017), SUM(td.f2018), SUM(td.f2019),
            SUM(td.f2020), SUM(td.f2021), SUM(td.f2022), SUM(td.f2023)
        FROM NAEI2023ds.t_Data td
        WHERE td.NFRCode_id     IN (SELECT NFRCode_id     FROM tmp_nfrcode_ids)
          AND td.SourceName_id  IN (SELECT SourceName_id  FROM tmp_source_ids)
          AND td.ActivityName_id IN (SELECT ActivityName_id FROM tmp_activity_ids)
        GROUP BY td.Pollutant_id;
    END LOOP;

    CLOSE cur_groups;

    INSERT INTO t_Group_Data (
        Group_id,
        Pollutant_id,
        f1970, f1971, f1972, f1973, f1974, f1975, f1976, f1977, f1978, f1979,
        f1980, f1981, f1982, f1983, f1984, f1985, f1986, f1987, f1988, f1989,
        f1990, f1991, f1992, f1993, f1994, f1995, f1996, f1997, f1998, f1999,
        f2000, f2001, f2002, f2003, f2004, f2005, f2006, f2007, f2008, f2009,
        f2010, f2011, f2012, f2013, f2014, f2015, f2016, f2017, f2018, f2019,
        f2020, f2021, f2022, f2023
    )
    SELECT
        Group_id,
        Pollutant_id,
        f1970, f1971, f1972, f1973, f1974, f1975, f1976, f1977, f1978, f1979,
        f1980, f1981, f1982, f1983, f1984, f1985, f1986, f1987, f1988, f1989,
        f1990, f1991, f1992, f1993, f1994, f1995, f1996, f1997, f1998, f1999,
        f2000, f2001, f2002, f2003, f2004, f2005, f2006, f2007, f2008, f2009,
        f2010, f2011, f2012, f2013, f2014, f2015, f2016, f2017, f2018, f2019,
        f2020, f2021, f2022, f2023
    FROM tmp_group_buffer
    ORDER BY Group_id, Pollutant_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_group_buffer;
    DROP TEMPORARY TABLE IF EXISTS tmp_nfrcode_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_source_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_activity_ids;
END;;
DELIMITER ;