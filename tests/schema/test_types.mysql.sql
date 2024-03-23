drop table if exists test_types;
create table test_types
(type_id bigint primary key not null auto_increment,
        type_person_id bigint not null,
        type_int int,
        type_bigint bigint,
        type_currency decimal(19,2),
        type_decimal decimal(19,2),
        type_double double,
        type_string varchar(255),
        type_string_32 varchar(32),
        type_string_with_default_val varchar(32) not null default "default_val",
        type_text text,
        type_date_created timestamp not null default current_timestamp,
        type_last_modified timestamp not null default current_timestamp on update current_timestamp,
        type_date date,
        type_datetime datetime,
        type_time time,
        type_url text);
