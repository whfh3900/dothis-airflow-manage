
def create_channel_data_raw_table(date):
    return f"""
        CREATE TABLE IF NOT EXISTS dothis_raw.channel_data_{date} (
        `CHANNEL_ID` char(48) NOT NULL DEFAULT '',
        `CHANNEL_NAME` char(255) DEFAULT NULL,
        `CHANNEL_DESCRIPTION` text,
        `CHANNEL_TAGS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_KEYWORDS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_TAGS` varchar(2000) DEFAULT NULL,
        `CHANNEL_COUNTRY` char(100) DEFAULT NULL,
        `CHANNEL_LINK` varchar(8000) DEFAULT NULL,
        `CHANNEL_SINCE` char(24) DEFAULT NULL,
        `CHANNEL_CLUSTER` smallint(6) NOT NULL DEFAULT '-1',
        `CHANNEL_THUMBNAIL` text,
        `CRAWLED_DATE` timestamp NULL DEFAULT NULL,
        `USER_ID` int(11) DEFAULT NULL,
        `channel_id_part` char(1) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
        """

def create_channel_data_ld_table(date):
    return f"""
        CREATE TABLE IF NOT EXISTS dothis_ld.channel_data_{date} (
        `CHANNEL_ID` char(48) NOT NULL DEFAULT '',
        `CHANNEL_NAME` char(255) DEFAULT NULL,
        `CHANNEL_DESCRIPTION` text,
        `CHANNEL_TAGS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_KEYWORDS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_TAGS` varchar(2000) DEFAULT NULL,
        `CHANNEL_COUNTRY` char(100) DEFAULT NULL,
        `CHANNEL_LINK` varchar(8000) DEFAULT NULL,
        `CHANNEL_SINCE` char(24) DEFAULT NULL,
        `CHANNEL_CLUSTER` smallint(6) NOT NULL DEFAULT '-1',
        `CHANNEL_THUMBNAIL` text,
        `CRAWLED_DATE` timestamp NULL DEFAULT NULL,
        `USER_ID` int(11) DEFAULT NULL,
        `channel_id_part` char(1) NOT NULL,
        PRIMARY KEY (`CHANNEL_ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
        """

def create_channel_data_temp_table(date):
    return f"""
        CREATE TABLE IF NOT EXISTS dothis_temp.channel_data_{date} (
        `CHANNEL_ID` char(48) NOT NULL DEFAULT '',
        `CHANNEL_NAME` char(255) DEFAULT NULL,
        `CHANNEL_DESCRIPTION` text,
        `CHANNEL_TAGS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_KEYWORDS` varchar(2000) DEFAULT NULL,
        `MAINLY_USED_TAGS` varchar(2000) DEFAULT NULL,
        `CHANNEL_COUNTRY` char(100) DEFAULT NULL,
        `CHANNEL_LINK` varchar(8000) DEFAULT NULL,
        `CHANNEL_SINCE` char(24) DEFAULT NULL,
        `CHANNEL_CLUSTER` smallint(6) NOT NULL DEFAULT '-1',
        `CHANNEL_THUMBNAIL` text,
        `CRAWLED_DATE` timestamp NULL DEFAULT NULL,
        `USER_ID` int(11) DEFAULT NULL,
        `channel_id_part` char(1) NOT NULL,
        PRIMARY KEY (`CHANNEL_ID`,`channel_id_part`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED
        /*!50500 PARTITION BY LIST  COLUMNS(channel_id_part)
        (PARTITION pub VALUES IN ('_') ENGINE = InnoDB,
        PARTITION phb VALUES IN ('-') ENGINE = InnoDB,
        PARTITION p0 VALUES IN ('0') ENGINE = InnoDB,
        PARTITION p1 VALUES IN ('1') ENGINE = InnoDB,
        PARTITION p2 VALUES IN ('2') ENGINE = InnoDB,
        PARTITION p3 VALUES IN ('3') ENGINE = InnoDB,
        PARTITION p4 VALUES IN ('4') ENGINE = InnoDB,
        PARTITION p5 VALUES IN ('5') ENGINE = InnoDB,
        PARTITION p6 VALUES IN ('6') ENGINE = InnoDB,
        PARTITION p7 VALUES IN ('7') ENGINE = InnoDB,
        PARTITION p8 VALUES IN ('8') ENGINE = InnoDB,
        PARTITION p9 VALUES IN ('9') ENGINE = InnoDB,
        PARTITION pa VALUES IN ('a') ENGINE = InnoDB,
        PARTITION pb VALUES IN ('b') ENGINE = InnoDB,
        PARTITION pc VALUES IN ('c') ENGINE = InnoDB,
        PARTITION pd VALUES IN ('d') ENGINE = InnoDB,
        PARTITION pe VALUES IN ('e') ENGINE = InnoDB,
        PARTITION pf VALUES IN ('f') ENGINE = InnoDB,
        PARTITION pg VALUES IN ('g') ENGINE = InnoDB,
        PARTITION ph VALUES IN ('h') ENGINE = InnoDB,
        PARTITION pi VALUES IN ('i') ENGINE = InnoDB,
        PARTITION pj VALUES IN ('j') ENGINE = InnoDB,
        PARTITION pk VALUES IN ('k') ENGINE = InnoDB,
        PARTITION pl VALUES IN ('l') ENGINE = InnoDB,
        PARTITION pm VALUES IN ('m') ENGINE = InnoDB,
        PARTITION pn VALUES IN ('n') ENGINE = InnoDB,
        PARTITION po VALUES IN ('o') ENGINE = InnoDB,
        PARTITION pp VALUES IN ('p') ENGINE = InnoDB,
        PARTITION pq VALUES IN ('q') ENGINE = InnoDB,
        PARTITION pr VALUES IN ('r') ENGINE = InnoDB,
        PARTITION ps VALUES IN ('s') ENGINE = InnoDB,
        PARTITION pt VALUES IN ('t') ENGINE = InnoDB,
        PARTITION pu VALUES IN ('u') ENGINE = InnoDB,
        PARTITION pv VALUES IN ('v') ENGINE = InnoDB,
        PARTITION pw VALUES IN ('w') ENGINE = InnoDB,
        PARTITION px VALUES IN ('x') ENGINE = InnoDB,
        PARTITION py VALUES IN ('y') ENGINE = InnoDB,
        PARTITION pz VALUES IN ('z') ENGINE = InnoDB) */;
        """