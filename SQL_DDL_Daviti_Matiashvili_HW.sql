create database subway_metro;
create schema if not exists subway;


--creating table station
create table if not exists subway.station(
	stationID  integer primary key generated always as identity,
	location   varchar(100),
	requiredNumberOfTrains  integer not null check(requiredNumberOfTrains>0),
	requiredNumberOfEmployees integer  not null check(requiredNumberOfEmployees>=0)
);

--creating table ticket
create table if not exists subway.ticket(
		ticketID  integer primary key generated always as identity,
		StationID  integer not null  references subway.station(stationid),
		purchaseDate date not null,
		expiry_Date date check(expiry_Date>purchaseDate),
		price integer check(price>0),
		Discount decimal check(Discount between 0 and 100),
		passengerName  varchar(50) not null
);

--creating table employee
create table if not exists subway.employee(
			employeeID	integer primary key generated always as identity,
			stationID integer not null  references subway.station(stationid),
			role  varchar(50) check(upper(role) in('STATION MANAGER','TRAIN OPERATOR',
		'MAINTENANCE TECHNICIAN','TICKETING AGENT','SECURITY OFFICER')),
			employeeName	varchar  not null,
			Salary integer check(salary>0)
);


--creating table train
create table if not exists subway.train(
			trainID integer primary key generated always as identity,
			trainName	varchar(50) not null,
			capacity integer check(capacity>0)
);

--creating table track
create table if not exists subway.track(
		trackID	integer primary key generated always as identity,
		installationDate date  default current_date check(installationDate<=current_date 
		and ((extract (year from installationdate) = 2000 and extract(month from installationdate) >= 1) 
    or extract(year from installationdate) >= 2001)),
		length  integer check(length>0),
		trackCondition varchar(50)
);

--creating table lines
create table if not exists subway.lines(
			lineID integer primary key generated always as identity,
			lineName  varchar(50),
			OperatingFrequency	time
);

--creating table tunnel
create table if not exists subway.tunnel(
		tunnelID integer primary key generated always as identity,
		lineID integer not null references subway.lines(lineid),
		length integer check(length>0),
		constructionDate date  default current_date check(constructionDate<=current_date),
		tunnelCondition varchar(50)
);

--creating table repair
create table if not exists subway.repair(
		repairID 	integer primary key generated  always as identity,
		description text,
		repairDate date default current_date,
		cost  decimal check(cost>0),
		trackID  integer  references subway.track(trackID),
		TrainID  integer references subway.train(trainID),
		StationID  integer  references subway.station(stationID),
		TunnelID  integer  references subway.tunnel(tunnelID)
);
--creating table stationLines
create table if not exists subway.stationLines(
			StationID integer references subway.station(stationID),
			lineID integer references subway.lines(lineID),
			StationOrder integer,
			primary key(stationID,lineID)
);
--creating table trackLines
create table if not exists  subway.trackLines(
		TrackID integer references  subway.track(trackID),
		LineID integer references subway.lines(lineID),
		TrackOrder integer,
		primary key(TrackID,LineID)		
);

--creating table trainStaion
create table if not exists subway.trainStation(
			TrainID integer references subway.train(trainID),
			StationID integer references subway.station(stationID),
			departureTime  time check(departureTime>arrivalTime),
			arrivalTime time,
			primary key(trainID,StationID)
);


--inserting values into table station
insert into subway.station("location",requirednumberoftrains,requirednumberofemployees)
	select 'New-York',123,1234
	where not exists(
	select 1 from subway.station s
	where upper(s.location)=upper('new-york') and s.requirednumberoftrains=123
	)
	returning *;


insert into subway.station("location",requirednumberoftrains,requirednumberofemployees)
	select 'Tbilisi',20,200
	where not exists(
	select 1 from subway.station s
	where upper(s.location)=upper('tbilisi') and s.requirednumberoftrains=20)
	returning*;



--inserting values into table ticket
insert into subway.ticket (stationid,purchasedate,expiry_date,price,discount,passengername)
	
		select
		(select stationid from subway.station s 
			where upper(s.location)=upper('new-york') and s.requirednumberoftrains=123),
			date '11-01-2021', date '12-01-2021',30, 12.65,varchar(50) 'Daviti Matiashvili'
			where not exists(
				select 1 from
				subway.ticket t 
				where t.stationID=(select s.stationID
									from subway.station s
									where upper(s.location)=upper('new-york') and s.requirednumberoftrains=123)
									and upper(t.passengername)=upper('daviti matiashvili')
			)
		returning *;
	
	
insert into subway.ticket (stationid, purchaseDate, expiry_Date, price, discount, passengerName)
		 select  
		 (select stationid from subway.station s 
				where upper(s.location)=upper('tbilisi') and s.requirednumberoftrains=20), 
				 date '2021-01-12', date '2021-01-13', 20, 40.5, varchar(50) 'Luka lomidze'
				where not exists(
				select 1 from subway.ticket t
				where t.stationid=(select s.stationID from subway.station s
								where upper(s.location)=upper('tbilisi') and s.requirednumberoftrains=20)
								and upper(t.passengerName)=upper('luka lomidze'))
			returning *;	


	
	
	
-- inserting values into table employee
insert into subway.employee(stationid,role,employeename,salary)
		select
		(select stationid from subway.station s 
			where upper(s.location)=upper('new-york') and s.requirednumberoftrains=123),
		varchar(50) 'maintenance technician',varchar 'noe lomidze',3000
		where not exists(
			select 1 from subway.employee e
			where e.stationid=(select stationid from subway.station s 
			where upper(s.location)=upper('new-york') and s.requirednumberoftrains=123)
			and upper(e.employeename)=upper('noe lomidze'))
		returning *;
		
		
insert into subway.employee(stationid,role,employeename,salary)
	
		select 
		(select stationid from subway.station s 
			where upper(s.location)=upper('tbilisi') and s.requirednumberoftrains=20),
		varchar(50) 'security officer', varchar 'cotne mamageishvili',3000
		where not exists(
			select 1 from subway.employee e
			where e.stationid=(select stationid from subway.station s 
			where upper(s.location)=upper('tbilisi') and s.requirednumberoftrains=20)
			and upper(e.employeename)=upper('cotne mamageishvili'))
		returning*;

	
--inserting values into table train	
insert into subway.train (trainname,capacity)
		select varchar(50) 'Easter express',200
			where not exists(
			select 1 
			from subway.train t
			where  upper(t.trainname)=upper('easter express') and t.capacity=200)
		returning *;

insert into subway.train(trainname,capacity)
		select varchar(50) 'trolly',300
		where not exists(
			select 1 
			from subway.train t
			where  upper(t.trainname)=upper('trolly') and t.capacity=300)
		returning *;
	
	
	
--inserting values into table Lines

insert into subway.lines(linename,operatingfrequency)	

		select varchar(50) 'Saburtalo Line',time '00:07:00'
		where not exists(
		select 1 from subway.lines l
		where upper(l.linename)=upper('saburtalo line') and l.operatingfrequency='00:07:00')
		returning *;
	
insert into subway.lines(linename,operatingfrequency)
		select varchar(50) 'Didube Line', time '00:08:00'
		where not exists(
		select 1 from subway.lines l
		where upper(l.linename)=upper('didube line') and l.operatingfrequency='00:08:00')
		returning *;	
	

--inserting values into table track
	
insert into subway.track(installationdate,length,trackcondition)
	
	select date '2020-01-10',254, varchar(50) 'needs repair'
	where not exists(
	select 1 from subway.track t
	where t.installationdate=date '2020-01-10' and t.length=254)
	returning *;

insert into subway.track (installationdate,length,trackcondition)
	
	select date '2021-10-11',221,varchar(50) 'good condition'
	where not exists(
	select 1 from subway.track t
	where t.installationdate=date '2021-10-11' and t.length=221)
		 returning* ;

		
		
-- inserting values into table tunnel
insert into subway.tunnel(lineid,length,tunnelcondition,constructiondate)

	select 
		(select lineid from subway.lines l
		where upper(l.linename)=upper(varchar(50) 'saburtalo line') and l.operatingfrequency=time '00:07:00'),
		250,varchar(50) 'good', date '2021-11-07'
		where not exists(
		select 1 from subway.tunnel t
		where t.lineid=(select lineid from subway.lines l
		where upper(l.linename)=upper('saburtalo line') and l.operatingfrequency='00:07:00')
		and t.constructiondate=date '2021-11-07'
		)
		returning *;
	
insert into subway.tunnel(lineid,length,tunnelcondition,constructiondate)
	
	select 
			(select lineid from subway.lines l
		where upper(l.linename)=upper(varchar(50) 'didube line') and l.operatingfrequency=time '00:08:00'),
		250,varchar(50) 'needs repair',date '2020-11-07'
		where not exists(
		select 1 from subway.tunnel t
		where t.lineid=(select lineid from subway.lines l
		where upper(l.linename)=upper(varchar(50) 'didube line') and l.operatingfrequency= time '00:08:00')
		and t.constructiondate=date '2020-11-07'
		)
		returning *;
	
	

--inserting values into table repair
insert into subway.repair(description,repairdate,"cost",trainid,stationid,tunnelid,trackid)
	
	select text 'Track Repair',date '2022-02-03',500,null,null,null,
		(select trackid from subway.track t
		where t.length=254 and t.installationdate=date '2020-01-10')
		where not exists(
		select r.trackid from subway.repair r
		where r.trackid=(select trackid from subway.track t
		where t.length=254 and t.installationdate=date '2020-01-10')
		and r.repairdate= date '2022-02-03')
		returning *;
	
	
insert into subway.repair(description,repairdate,"cost",trainid,stationid,tunnelid,trackid)
	
	select text 'Track Repair',date '2021-12-09',500,null,null,null,
		(select trackid from subway.track t
		where t.length=221 and t.installationdate=date '2021-10-11')
		where not exists(
		select r.trackid from subway.repair r
		where r.trackid=(select trackid from subway.track t
		where t.length=221 and t.installationdate=date '2021-10-11')
		and r.repairdate= date '2021-12-09')
		returning *;

	
--inserting values into table trainStation
insert into subway.trainstation(trainID,stationid,departuretime,arrivaltime)
		
	select (select trainID from subway.train t
			where upper(t.trainname)=upper(varchar(50) 'easter express') and t.capacity=200),
			(select StationID from subway.station s
			where upper(s.location)=upper(varchar(50) 'new-york') and s.requirednumberoftrains=123),
			time '08:00:00',
			time '07:50:00'
			where not exists(
			select 1 from subway.trainstation t
			where t.trainID=(select trainID from subway.train t
			where upper(t.trainname)=upper(varchar(50)'easter express') and t.capacity=200)
			and t.stationID=(select StationID from subway.station s
			where upper(s.location)=upper(varchar(50)'new-york') and s.requirednumberoftrains=123)
			and t.departuretime=time '08:00:00')
			returning *;

insert into subway.trainstation(trainID,stationid,departuretime,arrivaltime)
		
	select (select trainID from subway.train t
			where upper(t.trainname)=upper(varchar(50) 'trolly') and t.capacity=300),
			(select StationID from subway.station s
			where upper(s.location)=upper(varchar(50) 'tbilisi') and s.requirednumberoftrains=20),
			time '10:00:00',
			time '09:50:00'
			where not exists(
			select 1 from subway.trainstation t
			where t.trainID=(select trainID from subway.train t
			where upper(t.trainname)=upper(varchar(50)'trolly') and t.capacity=300)
			and t.stationID=(select StationID from subway.station s
			where upper(s.location)=upper(varchar(50) 'tbilisi') and s.requirednumberoftrains=20)
			and t.departuretime=time '10:00:00')
			returning *;


--inserting values into table  trackLines
insert into subway.tracklines(trackID,lineID,trackOrder)
			select(select t.trackId from subway.track t
				where t.installationdate=date '2020-01-10' and t.length=254),
				(select l.lineID from subway.lines l
				where upper(l.linename)=upper(varchar(50) 'saburtalo line') and l.operatingfrequency=time '00:07:00'),
				2
				where not exists(
				select 1 from subway.tracklines t
				where t.trackid=(select t.trackId from subway.track t
				where t.installationdate=date '2020-01-10' and t.length=254)
				and t.lineid=(select l.lineID from subway.lines l
				where upper(l.linename)=upper(varchar(50) 'saburtalo line') and l.operatingfrequency=time '00:07:00'))
				returning *;

insert into subway.tracklines(trackID,lineID,trackOrder)
		
			select(select trackId from subway.track t
				where t.installationdate=date '2021-10-11' and t.length=221),
				(select lineID from subway.lines l
				where upper(l.linename)=upper(varchar(50) 'didube line') and l.operatingfrequency=time '00:08:00'),
				3
				where not exists(
				select 1 from subway.tracklines t
				where t.trackid=(select t.trackId from subway.track t
				where t.installationdate=date '2021-10-11' and t.length=221)
				and t.lineid=(select l.lineID from subway.lines l
				where upper(l.linename)=upper(varchar(50)'didube line') and l.operatingfrequency= time '00:08:00'))
				returning *;
		

			
--inserting values into table stationLines

insert into subway.stationlines(stationid,lineid,stationorder)
		
		select(select stationid from subway.station s
				where upper(s.location)=upper(varchar(50) 'new-york') and s.requirednumberoftrains=123),
				(select lineid from subway.lines l 
				where upper(l.linename)=upper(varchar(50) 'saburtalo line') and l.operatingfrequency=time '00:07:00'),
				2
				where not exists(
				select 1 from subway.stationlines s 
				where s.stationid=(select stationid from subway.station s
				where upper(s.location)=upper(varchar(50)'new-york') and s.requirednumberoftrains=123)
				and s.lineid=(select lineid from subway.lines l 
				where upper(l.linename)=upper(varchar(50) 'saburtalo line') and l.operatingfrequency=time '00:07:00')
				)
				returning *;
	
insert into subway.stationlines(stationid,lineid,stationorder)
		
		select(select stationid from subway.station s
				where upper(s.location)=upper(varchar(50)'tbilisi') and s.requirednumberoftrains=20),
				(select lineid from subway.lines l 
				where upper(l.linename)=upper(varchar(50) 'didube line') and l.operatingfrequency=time '00:08:00'),
				2
				where not exists(
				select 1 from subway.stationlines s 
				where s.stationid=(select stationid from subway.station s
				where upper(s.location)=upper(varchar(50) 'tbilisi') and s.requirednumberoftrains=20)
				and s.lineid=(select lineid from subway.lines l 
				where upper(l.linename)=upper(varchar(50) 'didube line') and l.operatingfrequency=time '00:08:00')
				)
				returning *;
			
			

--adding record_ts into the tables 
alter table subway.station
add column if not exists record_ts date not null default current_date;


alter table subway.ticket
add column if not exists record_ts date  not null default current_date;
	
	
alter table subway.employee 
add column if not exists record_ts date not null default current_date;


alter table subway.lines 
add column if not exists record_ts date not null default current_date;


alter table subway.track 
add column  if not exists record_ts date not null default current_date;
	
	
alter table subway.tunnel 
add column if not exists record_ts date not null default current_date;


alter table subway.repair 
add column if not exists record_ts date not null default current_date;	 


alter table subway.trainstation 
add column if not exists record_ts date not null default current_date;

alter table subway.tracklines 
add column if not exists  record_ts date not null default current_date;


alter table subway.stationlines 
add column if not exists record_ts date not null default current_date;	

	
	
		
			
