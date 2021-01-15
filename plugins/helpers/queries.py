create_demo_r_pjanind3_table = """
create table if not exists demo_r_pjanind3(
    indic_de text not null,
    unit text not null,
    geo text not null,
    time integer not null,
    value float
);
"""


create_demo_r_pjangrp3_table = """
create table if not exists demo_r_pjangrp3(
    sex text not null,
    unit text not null,
    age text not null,
    geo text not null,
    time integer not null,
    value float
);
"""


create_ess9_table = """
drop table if exists ESS9;
create table if not exists ESS9 (
    idno int4,
    region text,
    regunit int2,
    cntry text,
    nwspol float8,
    netusoft float8,
    netustm float8,
    ppltrst float8,
    pplfair float8,
    pplhlp float8,
    polintr float8,
    psppsgva float8,
    actrolga float8,
    psppipla float8,
    cptppola float8,
    trstprl float8,
    trstlgl float8,
    trstplc float8,
    trstplt float8,
    trstprt float8,
    trstep float8,
    trstun float8,
    constraint ess9_pkey primary key (cntry, idno)
);
"""

copy_from_s3 = """COPY {table}
    FROM '/tmp/csv/{filename}.csv'
    DELIMITER ','
    CSV HEADER;
"""


