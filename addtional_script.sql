drop table if exists main.age_grp ;
drop table if exists main.conditions;

create table main.age_grp(
	age_grp_id integer PRIMARY KEY,
	age_grp_desc varchar
);
create table main.conditions(
	condition_type_id integer PRIMARY KEY,
	condition_type_nm varchar
);

insert into main.age_grp (age_grp_id,age_grp_desc)
values 
(0,'All ages'),
(1,'12 or less'),
(2,'13-19 years'),
(3,'20-39 years'),
(4,'40-59 years'),
(5,'60+ years')
;

insert into main.conditions (condition_type_id,condition_type_nm) 
values 
(11,'Acute Myocardial Infarction (AMI)'),
(22,'Chronic Obstructive Pulmonary Disease (COPD)'),
(33,'Heart Failure (HF)'),
(44,'Pneumonia'),
(55,'Coronary Artery Bypass Graft (CABG) Surgery'),
(66,'Total Hip Arthroplasty / Total Knee Arthroplasty (THA/TKA)'),
(77,'Sepsis'),
(88,'Chronic Kidney Disease / ESRD'),
(99,'Transplants'),
(100,'Other Conditions'),
;
