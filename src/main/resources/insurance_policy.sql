
drop database insurance_policy;

create database insurance_policy;

use insurance_policy;

create table policy (
  id int primary key AUTO_INCREMENT,
  customer_id int unique,
  validity_date date not null,
  territorial_scope varchar(255)  not null,
  insurer varchar(255) not null
)

create table contract (
  id int primary key AUTO_INCREMENT,
  type varchar(255) not null,
  amount_insured double not null,
  completion_date date not null,
  expiration_date date not null,
  annual_subscription double not null,
  policy_id int,
  foreign key (policy_id) references policy (id)
)

delete from policy;

insert into policy (customer_id, validity_date, territorial_scope, insurer) 
  values ('1','2017.01.01','Germany','Delta Insurance');
  
insert into policy (customer_id, validity_date, territorial_scope, insurer) 
  values ('2','2017.06.20','Germany','Delta Insurance');
  
insert into policy (customer_id, validity_date, territorial_scope, insurer) 
  values ('3','2017.07.01','United States of Amerika','Delta Insurance Health');

insert into contract (type, amount_insured, completion_date, expiration_date, annual_subscription, policy_id)
 values ('Accident Insurance', 25000, '2008.05.01','2020.05.01',25.56,2);
 
 insert into contract (type, amount_insured, completion_date, expiration_date, annual_subscription, policy_id)
 values ('Health Insurance', 200000.0, '2008.05.01','2020.05.01',500.50,2);
 
-- SELECT * FROM POLICY INNER JOIN CONTRACT ON POLICY.ID = CONTRACT.POLICY_ID WHERE POLICY.ID = 2;
 