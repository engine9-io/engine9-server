drop table if exists test_types;
create table test_types
(col_id bigint primary key not null auto_increment,
        col_person_id bigint not null,
        col_int int,
        col_bigint bigint,
        col_currency decimal(19,2),
        col_decimal decimal(19,2),
        col_double double,
        col_string varchar(255),
        col_string_32 varchar(32),
        col_string_default varchar(32) not null default "default_val",
        col_text text,
        col_date_created timestamp not null default current_timestamp,
        col_last_modified timestamp not null default current_timestamp on update current_timestamp,
        col_date date,
        col_datetime datetime,
        col_time time,
        col_url text);
