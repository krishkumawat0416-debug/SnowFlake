create database regex_db;

use regex_db;

create schema demo;
create schema testing;

create table test123(id int, name varchar(20));
insert into test123 values(1, 'abc'), (2, 'krish');
describe table test123; 

describe database regex_db;