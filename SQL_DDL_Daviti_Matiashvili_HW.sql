create database subway_metro;
create schema if not exists subway;

--creating table station
create table if not exists subway.station(
	stationID  integer primary key,
	location   varchar(100),
	requieredNumberOfTrains  integer ,
	requiredNumberOfEmployees integer 
);

alter table subway.station
add constraint chk_requiredNumbers check (requieredNumberOfTrains >= 0 AND requiredNumberOfEmployees >= 0);

alter table subway.station
add column record_ts date not null default current_date;


insert into subway_metro.subway.station(stationid,"location",requierednumberoftrains,requirednumberofemployees)

	select 1, 'New-York',123,1234
	returning *;
	
insert into subway_metro.subway.station(stationid,"location",requierednumberoftrains,requirednumberofemployees)
	select 2,'Tbilisi',20,200
	returning*;
	
select * from subway_metro.subway.station s;


--creating table ticket

create table if not exists subway.ticket(
		ticketID  integer primary key,
		StationID  integer not null  references subway_metro.subway.station,
		purchaseDate date not null,
		expiry_Date date,
		price integer,
		Discount decimal,
		passengerName  varchar(50) not null
);

/* if you take a ticket then it has to be at some station that's why stationID can be null also purchase date can't be null
 * and passengerName because if someone took a ticket it must have date and the name of person who took it.
 */

alter table subway.ticket 
add constraint  checker  check(expiry_Date>purchaseDate and Discount between 0 and 100 and price>0);

alter table subway.ticket
add column record_ts date  not null default current_date;

insert into subway_metro.subway.ticket (ticketid,stationid,purchasedate,expiry_date,price,discount,passengername)
	
		select 1,
		(select stationid from subway_metro.subway.station s 
			where s.stationid=1),
			'11-01-2021','12-01-2021',30, 12.65,'Daviti'
		returning *;
		
	insert into subway_metro.subway.ticket (ticketid, stationid, purchaseDate, expiry_Date, price, discount, passengerName)
	 select 2, 
	 (select stationid from subway_metro.subway.station s 
			where s.stationid=2), 
			'2021-01-12', '2021-01-13', 20, 40.5, 'Luka'
		returning *;
	
	
	/* in the notes it is written to avoid hardcoding but for example how can I write stationID instead of 1 in the select clause,
	 * I wrote it and used join but before we insert some actual data how can we use joins and avoid hardcoding? also I don't know 
	 * actual meaning of term hardcoding  and I will be grateful if you explain it to me.
	 */
	
	
	
	
--creating table Employee
	
create table if not exists subway_metro.subway.employee(
			employeeID	integer primary key,
			stationID integer not null  references subway_metro.subway.station,
			role  varchar(50),
			employeeName	varchar  not null,
			Salary integer
);

--adding some constraints on columns

alter table subway_metro.subway.employee
add constraint checker check(upper(role) in('STATION MANAGER','TRAIN OPERATOR',
		'MAINTENANCE TECHNICIAN','TICKETING AGENT','SECURITY OFFICER')
		 and salary>0);
		
alter table subway.employee 
add column record_ts date not null default current_date;
		 
/* in the task description it is written that some constraints should be like this-
 * inserted value that can only be a specific value (as an example of gender) 
 * and I guess constraint on column 'role' is that kind of constraint
 */
		 
insert into subway_metro.subway.employee(employeeid,stationid,role,employeename,salary)
	
		select 1,
		(select stationid from subway_metro.subway.station s 
			where s.stationid=1),
		'maintenance technician','daviti',3000
		returning;
		
		
insert into subway_metro.subway.employee(employeeid,stationid,role,employeename,salary)
	
		select 2,
		(select stationid from subway_metro.subway.station s 
			where s.stationid=2),
		'security officer','cotne',3000
		returning*;
	


--creating table train
	
create table if not exists subway_metro.subway.train(
			trainID		integer primary key,
			trainName	varchar(50) not null,
			capacity integer
);

--we don't need train with no capacity
alter table subway.train
add constraint checker check(capacity>0);

alter table subway.train 
add constraint checker1 unique(trainName);

alter table subway.train
add column record_ts date not null default current_date


insert into subway.train (trainid,trainname,capacity)
		
		select 1,upper('Easter express'),200
		returning *;

insert into subway.train(trainid,trainname,capacity)
		select 2,upper('trolly'),300
		returning *;

		
--creating table lines 

create table if not exists subway.lines(
			lineID integer primary key ,
			lineName  varchar(50),
			OperatingFrequency	time
);

alter table subway.lines 
add column record_ts date not null default current_date;

-- in the first homework I didn't have any constraints on this table so I'm going to skip adding constraints part on this one

		insert into subway.lines(lineid,linename,operatingfrequency)
			
		select 1,'Saburtalo Line','07:00'
		returning *;
	
		insert into subway.lines(lineid,linename,operatingfrequency)
		select 2,'Didube Line','08:00'
		returning *;
	

--creating table track
	
create table if not exists subway.track(
		trackID	integer primary key,
		installationDate date default current_date,
		length  integer,
		trackCondition varchar(50)
);


alter table subway.track 
add constraint checker check(installationDate<=current_date and length>0 );

alter table subway.track 
add constraint second_checker2 
check (
    (extract(year from installationdate) = 2000 and extract(month from installationdate) >= 1) 
    or extract(year from installationdate) >= 2001
);

alter table subway.track 
add column  record_ts date not null default current_date;


/* I forgot to use this constraint and I have already runned 'checker' so I added second one to check 
 * if every tracks installation date is greater than  1 january 2000
 */

insert into subway.track(trackid,installationdate,length,trackcondition)
	
	select 1,'2020-01-10',254,'needs repair'
	returning *;

insert into subway.track (trackid,installationdate,length,trackcondition)
	
	select 2,'2021-10-11',221,'good condition'
		 returning* ;

		

--creating table tunnel
		
create table if not exists subway.tunnel(
		tunnelID integer primary key,
		lineID integer not null references subway.lines,
		length integer ,
		constructionDate date  default current_date,
		tunnelCondition varchar(50)
);

alter table subway.tunnel 
add constraint checker check(constructionDate<=current_date and length>0);

alter table subway.tunnel 
add column record_ts date not null default current_date;

insert into subway.tunnel(tunnelid,lineid,length,tunnelcondition,constructiondate)

	
	select 1,
		(select lineid from subway.lines l
		where l.lineid=1),
		250,'good','2021-11-07'
		returning *;
	
insert into subway.tunnel(tunnelid,lineid,length,tunnelcondition,constructiondate)
	
	select 2,
		(select lineid from subway.lines l
		where l.lineid=2),
		250,'needs repair','2020-11-07'
		returning *;


		
		 
--creating table repair
create table if not exists subway.repair(
		repairID 	integer primary key,
		description text,
		repairDate date default current_date,
		cost  decimal,
		trackID  integer  references subway.track,
		TrainID  integer references subway.train,
		StationID  integer  references subway.station,
		TunnelID  integer  references subway.tunnel
)


alter table subway.repair 
add constraint checker check(cost>0);

alter table subway.repair 
add column record_ts date not null default current_date;


insert into subway.repair(repairid,description,"cost",trainid,stationid,tunnelid,trackid)
	
	select 1, 'Track Repair',500,null,null,null,
		(select trackid from subway.track t
		where t.trackid=1)
		returning *;
	
insert into subway.repair(repairid,description,"cost",trainid,stationid,tunnelid,trackid)
	
	select 2, 'Track Repair',500,null,null,null,
		(select trackid from subway.track t
		where t.trackid=2)
		returning *;
	

-- creating bridge table TrainStation
	
create table if not exists subway.trainStation(
			TrainID integer references subway.train,
			StationID integer references subway.station,
			departureTime  time,
			arrivalTime time,
			primary key(trainID,StationID)
);

-- adding one logical constraint 
alter table subway.trainstation
add constraint checker check(departuretime>ArrivalTime);

alter table subway.trainstation 
add column record_ts date not null default current_date;

insert into subway.trainstation(trainID,stationid,departuretime,arrivaltime)
		
	select (select trainID from subway.train t
			where t.trainid=1),
			(select StationID from subway.station s
			where s.stationid=1),
			'08:00:00',
			'07:50:00'
			returning *;

insert into subway.trainstation(trainID,stationid,departuretime,arrivaltime)
		
	select (select trainID from subway.train t
			where t.trainid=1),
			(select StationID from subway.station s
			where s.stationid=2),
			'10:00:00',
			'09:50:00'
			returning *;
		
--creating bridge table TrackLines
		
		
create table if not exists  subway.trackLines(
		TrackID integer references  subway.track,
		LineID integer references subway.lines,
		TrackOrder integer,
		primary key(TrackID,LineID)		
);
alter table subway.tracklines 
add column record_ts date not null default current_date;

-- i had 0 constraint on this table

insert into subway.tracklines(trackID,lineID,trackOrder)
		
			select(select trackId from subway.track t
				where t.trackid=1),
				(select lineID from subway.lines l
				where l.lineID=2),
				2
				returning *;

insert into subway.tracklines(trackID,lineID,trackOrder)
		
			select(select trackId from subway.track t
				where t.trackid=2),
				(select lineID from subway.lines l
				where l.lineID=2),
				3
				returning *;
		
			
--creating bridge table  StationLines
			
create table if not exists subway.stationLines(
			StationID integer references subway.station,
			lineID integer references subway.lines,
			StationOrder integer,
			primary key(stationID,lineID)
);

alter table subway.stationlines 
add column record_ts date not null default current_date;

-- i don't have any constraint on this table

	insert into subway.stationlines(stationid,lineid,stationorder)
		
		select(select stationid from subway.station s
				where s.stationid=1),
				(select lineid from subway.lines l 
				where l.lineid=1),
				2
				returning *;
	
	insert into subway.stationlines(stationid,lineid,stationorder)
		
		select(select stationid from subway.station s
				where s.stationid=2),
				(select lineid from subway.lines l 
				where l.lineid=2),
				2
				returning *;
			
			select * from subway.stationlines s 
	
		
			

