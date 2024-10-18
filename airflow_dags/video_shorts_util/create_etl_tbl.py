import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv
sys.path.append('/Users/byungwoyoon/Desktop/Projects/dothis-airflow/airflow-manage/airflow_dags')
from util.db_util import get_mysql_connector
sys.path.append('./')

load_dotenv()

def video_data_shorts_raw_ddl() -> str:
    return f"""
    create table if not exists `video_data_shorts_{datetime.today().strftime("%Y%m%d")}` (
    `video_id` varchar(11) NOT NULL,
    `channel_id` varchar(50) NOT NULL,
    `video_title` mediumtext,
    `video_description` mediumtext,
    `video_tags` mediumtext,
    `video_duration` int(11) DEFAULT '0',
    `video_published` date DEFAULT NULL,
    `video_category` varchar(50) DEFAULT '',
    `video_info_card` tinyint(1) DEFAULT NULL,
    `video_with_ads` tinyint(1) DEFAULT NULL,
    `video_end_screen` tinyint(1) DEFAULT NULL,
    `video_cluster` smallint(6) DEFAULT NULL,
    `crawled_date` timestamp NULL DEFAULT NULL,
    `year` smallint(6) DEFAULT NULL,
    `month` smallint(6) DEFAULT NULL,
    `day` smallint(6) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
    """

def video_data_shorts_ld_ddl() -> str:
    return f"""
    create table if not exists `video_data_shorts_{datetime.today().strftime("%Y%m%d")}` (
    video_id varchar(11) NOT NULL,
    channel_id varchar(52) NOT NULL,
    video_title varchar(2000),
    video_description text,
    video_tags varchar(4000),
    video_duration int(6) DEFAULT '0',
    video_published date DEFAULT NULL,
    video_category varchar(50) DEFAULT '',
    video_info_card int(1) DEFAULT NULL,
    video_with_ads int(1) DEFAULT NULL,
    video_end_screen int(1) DEFAULT NULL,
    video_cluster int(6) DEFAULT NULL,
    crawled_date timestamp NULL DEFAULT NULL,
    year int(4) DEFAULT NULL,
    month int(2) DEFAULT NULL,
    day int(2) DEFAULT NULL
    )ENGINE=INNODB CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=COMPRESSED;
    """


def video_data_shorts_temp_ddl() -> str:
    return f"""
    create table if not exists `video_data_shorts_{datetime.today().strftime("%Y%m%d")}` (
    video_id varchar(11) NOT NULL,
    channel_id varchar(52) NOT NULL,
    video_title varchar(2000),
    video_description text,
    video_tags varchar(4000),
    video_duration int(6) DEFAULT '0',
    video_published date DEFAULT NULL,
    video_category varchar(50) DEFAULT '',
    video_info_card int(1) DEFAULT NULL,
    video_with_ads int(1) DEFAULT NULL,
    video_end_screen int(1) DEFAULT NULL,
    video_cluster int(6) DEFAULT NULL,
    crawled_date timestamp NULL DEFAULT NULL,
    year int(4) NOT NULL DEFAULT 2000,
    month int(2) NOT NULL DEFAULT 1,
    day int(2) NOT NULL DEFAULT 1,
    PRIMARY KEY (`video_id`,`year`,`month`,`day`)
    )
    PARTITION BY RANGE (`year` * 100 + `month`)
    SUBPARTITION BY HASH (`day`)
    SUBPARTITIONS 31
    (PARTITION `p202001` VALUES LESS THAN (202002),
    PARTITION `p202002` VALUES LESS THAN (202003),
    PARTITION `p202003` VALUES LESS THAN (202004),
    PARTITION `p202004` VALUES LESS THAN (202005),
    PARTITION `p202005` VALUES LESS THAN (202006),
    PARTITION `p202006` VALUES LESS THAN (202007),
    PARTITION `p202007` VALUES LESS THAN (202008),
    PARTITION `p202008` VALUES LESS THAN (202009),
    PARTITION `p202009` VALUES LESS THAN (202010),
    PARTITION `p202010` VALUES LESS THAN (202011),
    PARTITION `p202011` VALUES LESS THAN (202012),
    PARTITION `p202012` VALUES LESS THAN (202101),
    PARTITION `p202101` VALUES LESS THAN (202102),
    PARTITION `p202102` VALUES LESS THAN (202103),
    PARTITION `p202103` VALUES LESS THAN (202104),
    PARTITION `p202104` VALUES LESS THAN (202105),
    PARTITION `p202105` VALUES LESS THAN (202106),
    PARTITION `p202106` VALUES LESS THAN (202107),
    PARTITION `p202107` VALUES LESS THAN (202108),
    PARTITION `p202108` VALUES LESS THAN (202109),
    PARTITION `p202109` VALUES LESS THAN (202110),
    PARTITION `p202110` VALUES LESS THAN (202111),
    PARTITION `p202111` VALUES LESS THAN (202112),
    PARTITION `p202112` VALUES LESS THAN (202201),
    PARTITION `p202201` VALUES LESS THAN (202202),
    PARTITION `p202202` VALUES LESS THAN (202203),
    PARTITION `p202203` VALUES LESS THAN (202204),
    PARTITION `p202204` VALUES LESS THAN (202205),
    PARTITION `p202205` VALUES LESS THAN (202206),
    PARTITION `p202206` VALUES LESS THAN (202207),
    PARTITION `p202207` VALUES LESS THAN (202208),
    PARTITION `p202208` VALUES LESS THAN (202209),
    PARTITION `p202209` VALUES LESS THAN (202210),
    PARTITION `p202210` VALUES LESS THAN (202211),
    PARTITION `p202211` VALUES LESS THAN (202212),
    PARTITION `p202212` VALUES LESS THAN (202301),
    PARTITION `p202301` VALUES LESS THAN (202302),
    PARTITION `p202302` VALUES LESS THAN (202303),
    PARTITION `p202303` VALUES LESS THAN (202304),
    PARTITION `p202304` VALUES LESS THAN (202305),
    PARTITION `p202305` VALUES LESS THAN (202306),
    PARTITION `p202306` VALUES LESS THAN (202307),
    PARTITION `p202307` VALUES LESS THAN (202308),
    PARTITION `p202308` VALUES LESS THAN (202309),
    PARTITION `p202309` VALUES LESS THAN (202310),
    PARTITION `p202310` VALUES LESS THAN (202311),
    PARTITION `p202311` VALUES LESS THAN (202312),
    PARTITION `p202312` VALUES LESS THAN (202401),
    PARTITION `p202401` VALUES LESS THAN (202402),
    PARTITION `p202402` VALUES LESS THAN (202403),
    PARTITION `p202403` VALUES LESS THAN (202404),
    PARTITION `p202404` VALUES LESS THAN (202405),
    PARTITION `p202405` VALUES LESS THAN (202406),
    PARTITION `p202406` VALUES LESS THAN (202407),
    PARTITION `p202407` VALUES LESS THAN (202408),
    PARTITION `p202408` VALUES LESS THAN (202409),
    PARTITION `p202409` VALUES LESS THAN (202410),
    PARTITION `p202410` VALUES LESS THAN (202411),
    PARTITION `p202411` VALUES LESS THAN (202412),
    PARTITION `p202412` VALUES LESS THAN (202501),
    PARTITION `p202501` VALUES LESS THAN (202502),
    PARTITION `p202502` VALUES LESS THAN (202503),
    PARTITION `p202503` VALUES LESS THAN (202504),
    PARTITION `p202504` VALUES LESS THAN (202505),
    PARTITION `p202505` VALUES LESS THAN (202506),
    PARTITION `p202506` VALUES LESS THAN (202507),
    PARTITION `p202507` VALUES LESS THAN (202508),
    PARTITION `p202508` VALUES LESS THAN (202509),
    PARTITION `p202509` VALUES LESS THAN (202510),
    PARTITION `p202510` VALUES LESS THAN (202511),
    PARTITION `p202511` VALUES LESS THAN (202512),
    PARTITION `p202512` VALUES LESS THAN (202601),
    PARTITION `p202601` VALUES LESS THAN (202602),
    PARTITION `p202602` VALUES LESS THAN (202603),
    PARTITION `p202603` VALUES LESS THAN (202604),
    PARTITION `p202604` VALUES LESS THAN (202605),
    PARTITION `p202605` VALUES LESS THAN (202606),
    PARTITION `p202606` VALUES LESS THAN (202607),
    PARTITION `p202607` VALUES LESS THAN (202608),
    PARTITION `p202608` VALUES LESS THAN (202609),
    PARTITION `p202609` VALUES LESS THAN (202610),
    PARTITION `p202610` VALUES LESS THAN (202611),
    PARTITION `p202611` VALUES LESS THAN (202612),
    PARTITION `p202612` VALUES LESS THAN (202701),
    PARTITION `p202701` VALUES LESS THAN (202702),
    PARTITION `p202702` VALUES LESS THAN (202703),
    PARTITION `p202703` VALUES LESS THAN (202704),
    PARTITION `p202704` VALUES LESS THAN (202705),
    PARTITION `p202705` VALUES LESS THAN (202706),
    PARTITION `p202706` VALUES LESS THAN (202707),
    PARTITION `p202707` VALUES LESS THAN (202708),
    PARTITION `p202708` VALUES LESS THAN (202709),
    PARTITION `p202709` VALUES LESS THAN (202710),
    PARTITION `p202710` VALUES LESS THAN (202711),
    PARTITION `p202711` VALUES LESS THAN (202712),
    PARTITION `p202712` VALUES LESS THAN (202801),
    PARTITION `p202801` VALUES LESS THAN (202802),
    PARTITION `p202802` VALUES LESS THAN (202803),
    PARTITION `p202803` VALUES LESS THAN (202804),
    PARTITION `p202804` VALUES LESS THAN (202805),
    PARTITION `p202805` VALUES LESS THAN (202806),
    PARTITION `p202806` VALUES LESS THAN (202807),
    PARTITION `p202807` VALUES LESS THAN (202808),
    PARTITION `p202808` VALUES LESS THAN (202809),
    PARTITION `p202809` VALUES LESS THAN (202810),
    PARTITION `p202810` VALUES LESS THAN (202811),
    PARTITION `p202811` VALUES LESS THAN (202812),
    PARTITION `p202812` VALUES LESS THAN (202901),
    PARTITION `p202901` VALUES LESS THAN (202902),
    PARTITION `p202902` VALUES LESS THAN (202903),
    PARTITION `p202903` VALUES LESS THAN (202904),
    PARTITION `p202904` VALUES LESS THAN (202905),
    PARTITION `p202905` VALUES LESS THAN (202906),
    PARTITION `p202906` VALUES LESS THAN (202907),
    PARTITION `p202907` VALUES LESS THAN (202908),
    PARTITION `p202908` VALUES LESS THAN (202909),
    PARTITION `p202909` VALUES LESS THAN (202910),
    PARTITION `p202910` VALUES LESS THAN (202911),
    PARTITION `p202911` VALUES LESS THAN (202912),
    PARTITION `p202912` VALUES LESS THAN (203001),
    PARTITION `p203001` VALUES LESS THAN (203002),
    PARTITION `p203002` VALUES LESS THAN (203003),
    PARTITION `p203003` VALUES LESS THAN (203004),
    PARTITION `p203004` VALUES LESS THAN (203005),
    PARTITION `p203005` VALUES LESS THAN (203006),
    PARTITION `p203006` VALUES LESS THAN (203007),
    PARTITION `p203007` VALUES LESS THAN (203008),
    PARTITION `p203008` VALUES LESS THAN (203009),
    PARTITION `p203009` VALUES LESS THAN (203010),
    PARTITION `p203010` VALUES LESS THAN (203011),
    PARTITION `p203011` VALUES LESS THAN (203012),
    PARTITION `p203012` VALUES LESS THAN (203101),
    PARTITION `p203101` VALUES LESS THAN (203102),
    PARTITION `p203102` VALUES LESS THAN (203103),
    PARTITION `p203103` VALUES LESS THAN (203104),
    PARTITION `p203104` VALUES LESS THAN (203105),
    PARTITION `p203105` VALUES LESS THAN (203106),
    PARTITION `p203106` VALUES LESS THAN (203107),
    PARTITION `p203107` VALUES LESS THAN (203108),
    PARTITION `p203108` VALUES LESS THAN (203109),
    PARTITION `p203109` VALUES LESS THAN (203110),
    PARTITION `p203110` VALUES LESS THAN (203111),
    PARTITION `p203111` VALUES LESS THAN (203112),
    PARTITION `p203112` VALUES LESS THAN (203201),
    PARTITION `p203201` VALUES LESS THAN (203202),
    PARTITION `p203202` VALUES LESS THAN (203203),
    PARTITION `p203203` VALUES LESS THAN (203204),
    PARTITION `p203204` VALUES LESS THAN (203205),
    PARTITION `p203205` VALUES LESS THAN (203206),
    PARTITION `p203206` VALUES LESS THAN (203207),
    PARTITION `p203207` VALUES LESS THAN (203208),
    PARTITION `p203208` VALUES LESS THAN (203209),
    PARTITION `p203209` VALUES LESS THAN (203210),
    PARTITION `p203210` VALUES LESS THAN (203211),
    PARTITION `p203211` VALUES LESS THAN (203212),
    PARTITION `p203212` VALUES LESS THAN (203301),
    PARTITION `p203301` VALUES LESS THAN (203302),
    PARTITION `p203302` VALUES LESS THAN (203303),
    PARTITION `p203303` VALUES LESS THAN (203304),
    PARTITION `p203304` VALUES LESS THAN (203305),
    PARTITION `p203305` VALUES LESS THAN (203306),
    PARTITION `p203306` VALUES LESS THAN (203307),
    PARTITION `p203307` VALUES LESS THAN (203308),
    PARTITION `p203308` VALUES LESS THAN (203309),
    PARTITION `p203309` VALUES LESS THAN (203310),
    PARTITION `p203310` VALUES LESS THAN (203311),
    PARTITION `p203311` VALUES LESS THAN (203312),
    PARTITION `p203312` VALUES LESS THAN (203401),
    PARTITION `p203401` VALUES LESS THAN (203402),
    PARTITION `p203402` VALUES LESS THAN (203403),
    PARTITION `p203403` VALUES LESS THAN (203404),
    PARTITION `p203404` VALUES LESS THAN (203405),
    PARTITION `p203405` VALUES LESS THAN (203406),
    PARTITION `p203406` VALUES LESS THAN (203407),
    PARTITION `p203407` VALUES LESS THAN (203408),
    PARTITION `p203408` VALUES LESS THAN (203409),
    PARTITION `p203409` VALUES LESS THAN (203410),
    PARTITION `p203410` VALUES LESS THAN (203411),
    PARTITION `p203411` VALUES LESS THAN (203412),
    PARTITION `p203412` VALUES LESS THAN (203501),
    PARTITION `p203501` VALUES LESS THAN (203502),
    PARTITION `p203502` VALUES LESS THAN (203503),
    PARTITION `p203503` VALUES LESS THAN (203504),
    PARTITION `p203504` VALUES LESS THAN (203505),
    PARTITION `p203505` VALUES LESS THAN (203506),
    PARTITION `p203506` VALUES LESS THAN (203507),
    PARTITION `p203507` VALUES LESS THAN (203508),
    PARTITION `p203508` VALUES LESS THAN (203509),
    PARTITION `p203509` VALUES LESS THAN (203510),
    PARTITION `p203510` VALUES LESS THAN (203511),
    PARTITION `p203511` VALUES LESS THAN (203512),
    PARTITION `p203512` VALUES LESS THAN (203601),
    PARTITION `p203601` VALUES LESS THAN (203602),
    PARTITION `p203602` VALUES LESS THAN (203603),
    PARTITION `p203603` VALUES LESS THAN (203604),
    PARTITION `p203604` VALUES LESS THAN (203605),
    PARTITION `p203605` VALUES LESS THAN (203606),
    PARTITION `p203606` VALUES LESS THAN (203607),
    PARTITION `p203607` VALUES LESS THAN (203608),
    PARTITION `p203608` VALUES LESS THAN (203609),
    PARTITION `p203609` VALUES LESS THAN (203610),
    PARTITION `p203610` VALUES LESS THAN (203611),
    PARTITION `p203611` VALUES LESS THAN (203612),
    PARTITION `p203612` VALUES LESS THAN (203701),
    PARTITION `p203701` VALUES LESS THAN (203702),
    PARTITION `p203702` VALUES LESS THAN (203703),
    PARTITION `p203703` VALUES LESS THAN (203704),
    PARTITION `p203704` VALUES LESS THAN (203705),
    PARTITION `p203705` VALUES LESS THAN (203706),
    PARTITION `p203706` VALUES LESS THAN (203707),
    PARTITION `p203707` VALUES LESS THAN (203708),
    PARTITION `p203708` VALUES LESS THAN (203709),
    PARTITION `p203709` VALUES LESS THAN (203710),
    PARTITION `p203710` VALUES LESS THAN (203711),
    PARTITION `p203711` VALUES LESS THAN (203712),
    PARTITION `p203712` VALUES LESS THAN (203801),
    PARTITION `p203801` VALUES LESS THAN (203802),
    PARTITION `p203802` VALUES LESS THAN (203803),
    PARTITION `p203803` VALUES LESS THAN (203804),
    PARTITION `p203804` VALUES LESS THAN (203805),
    PARTITION `p203805` VALUES LESS THAN (203806),
    PARTITION `p203806` VALUES LESS THAN (203807),
    PARTITION `p203807` VALUES LESS THAN (203808),
    PARTITION `p203808` VALUES LESS THAN (203809),
    PARTITION `p203809` VALUES LESS THAN (203810),
    PARTITION `p203810` VALUES LESS THAN (203811),
    PARTITION `p203811` VALUES LESS THAN (203812),
    PARTITION `p203812` VALUES LESS THAN (203901));
    """

def video_history_shorts_raw_ddl() -> str:
    return f"""
    create table if not exists `video_history_shorts_{datetime.today().strftime("%Y%m%d")}` (
    video_id varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
    video_views int(20) DEFAULT '0' NULL,
    video_likes int(20) DEFAULT '0' NULL,
    video_comments int(20) DEFAULT '0',
    video_performance double DEFAULT '0',
    year int(4) DEFAULT NULL,
    month int(2) DEFAULT NULL,
    day int(2) DEFAULT NULL,
    video_cluster int(6) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
    """

def video_history_shorts_ld_ddl() -> str:
    return f"""
    create table if not exists `video_history_shorts_{datetime.today().strftime("%Y%m%d")}` (
    video_id varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
    video_views int(20) DEFAULT '0' NULL,
    video_likes int(20) DEFAULT '0' NULL,
    video_comments int(20) DEFAULT '0',
    video_performance double DEFAULT '0',
    year int(4) DEFAULT 2000 NOT NULL,
    month int(2) DEFAULT 01 NOT NULL,
    day int(2) DEFAULT 01 NOT NULL,
    video_cluster int(6) DEFAULT NULL,
    primary KEY (video_id,year,month,day)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
    """

def video_history_shorts_temp_ddl() -> str:
    date = datetime.today().strftime("%Y%m%d")
    year_month = date[:6]
    return f"""
    create table if not exists `video_history_shorts_{date}` (
    video_id varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
    video_views int(20) DEFAULT '0' NULL,
    video_likes int(20) DEFAULT '0' NULL,
    video_comments int(20) DEFAULT '0',
    video_performance double DEFAULT '0',
    year int(4) DEFAULT 2000 NOT NULL,
    month int(2) DEFAULT 01 NOT NULL,
    day int(2) DEFAULT 01 NOT NULL,
    video_cluster int(6) DEFAULT NULL,  
    PRIMARY KEY (`video_id`,`year`,`month`,`day`)
    )
    PARTITION BY RANGE (`year` * 10000 + `month` * 100 + `day`)
    (PARTITION `p{year_month}01` VALUES LESS THAN ({year_month}01),
    PARTITION `p{year_month}02` VALUES LESS THAN ({year_month}02),
    PARTITION `p{year_month}03` VALUES LESS THAN ({year_month}03),
    PARTITION `p{year_month}04` VALUES LESS THAN ({year_month}04),
    PARTITION `p{year_month}05` VALUES LESS THAN ({year_month}05),
    PARTITION `p{year_month}06` VALUES LESS THAN ({year_month}06),
    PARTITION `p{year_month}07` VALUES LESS THAN ({year_month}07),
    PARTITION `p{year_month}08` VALUES LESS THAN ({year_month}08),
    PARTITION `p{year_month}09` VALUES LESS THAN ({year_month}09),
    PARTITION `p{year_month}10` VALUES LESS THAN ({year_month}10),
    PARTITION `p{year_month}11` VALUES LESS THAN ({year_month}11),
    PARTITION `p{year_month}12` VALUES LESS THAN ({year_month}12),
    PARTITION `p{year_month}13` VALUES LESS THAN ({year_month}13),
    PARTITION `p{year_month}14` VALUES LESS THAN ({year_month}14),
    PARTITION `p{year_month}15` VALUES LESS THAN ({year_month}15),
    PARTITION `p{year_month}16` VALUES LESS THAN ({year_month}16),
    PARTITION `p{year_month}17` VALUES LESS THAN ({year_month}17),
    PARTITION `p{year_month}18` VALUES LESS THAN ({year_month}18),
    PARTITION `p{year_month}19` VALUES LESS THAN ({year_month}19),
    PARTITION `p{year_month}20` VALUES LESS THAN ({year_month}20),
    PARTITION `p{year_month}21` VALUES LESS THAN ({year_month}21),
    PARTITION `p{year_month}22` VALUES LESS THAN ({year_month}22),
    PARTITION `p{year_month}23` VALUES LESS THAN ({year_month}23),
    PARTITION `p{year_month}24` VALUES LESS THAN ({year_month}24),
    PARTITION `p{year_month}25` VALUES LESS THAN ({year_month}25),
    PARTITION `p{year_month}26` VALUES LESS THAN ({year_month}26),
    PARTITION `p{year_month}27` VALUES LESS THAN ({year_month}27),
    PARTITION `p{year_month}28` VALUES LESS THAN ({year_month}28),
    PARTITION `p{year_month}29` VALUES LESS THAN ({year_month}29),
    PARTITION `p{year_month}30` VALUES LESS THAN ({year_month}30),
    PARTITION `p{year_month}31` VALUES LESS THAN ({year_month}31));
    """





### 데일리 테이블 생성
def create_video_history_shorts_etl_tbl():
    print("video_history etl table create begin")
    connector = get_mysql_connector("dothis_raw", None)
    try:
        cursor = connector.cursor()
        # raw
        cursor.execute("USE dothis_raw")
        cursor.execute(video_history_shorts_raw_ddl())
        connector.commit()
        # ld
        cursor.execute("USE dothis_ld")
        cursor.execute(video_history_shorts_ld_ddl())
        connector.commit()
        # temp
        cursor.execute("USE dothis_temp")
        cursor.execute(video_history_shorts_temp_ddl())
        connector.commit()
        print("video_history etl table create done")
    except Exception:
        print(str(traceback.format_exc()))
        print("video_history_shorts_etl table not created.")
    connector.close()
    
### 데일리 테이블 생성
def create_video_data_shorts_etl_tbl():
    print("video_data etl table create begin")
    connector = get_mysql_connector("dothis_raw", None)
    try:
        cursor = connector.cursor()
        # raw
        cursor.execute("USE dothis_raw")
        cursor.execute(video_data_shorts_raw_ddl())
        connector.commit()
        # ld
        cursor.execute("USE dothis_ld")
        cursor.execute(video_data_shorts_ld_ddl())
        connector.commit()
        # temp
        cursor.execute("USE dothis_temp")
        cursor.execute(video_data_shorts_temp_ddl())
        connector.commit()
        print("video_data etl table create done")
    except Exception:
        print(str(traceback.format_exc()))
        print("video_data_shorts_etl table not created.")
    connector.close()