create database museum;
create schema if not exists museumschema;



create table if not exists museumschema.museum(
		museumID integer primary key generated always as identity,
		name varchar,
		location varchar,
		open_date date
);


create table if not exists museumschema.visitor(
		visitorID integer primary key generated always as identity,
		first_name varchar not null,
		last_name varchar not null,
		fullname varchar not null	generated always as (last_name || ' ' || first_name) stored,
		email varchar 
);




create table if not exists museumschema.employee(
		employeeID integer primary key generated always as identity,
		first_name varchar not null ,
		last_name varchar not null,
		role varchar,
		museumID integer  references museumschema.museum(museumid)
);


create table if not exists museumschema.storage(
			storageID integer primary key generated always as identity,
			location varchar,
			capacity integer,
			museumID integer references museumschema.museum(museumid)
);



create table if not exists museumschema.exhibition(
		exhibitionID integer primary key generated always as identity,
		start_date date check(start_date<end_date),
		end_date date,
		is_online boolean,
		museumID integer references museumschema.museum(museumid)
);

create table if not exists museumschema.item(
		itemID	integer primary key generated always as identity,
		storageID integer references museumschema.storage(storageid),
		category varchar,
		last_updated timestamp default current_date,
		inMuseum boolean,
		description text,
		name varchar not null
);

create table if not exists museumschema.museum_visitor(
		visitorID integer references museumschema.visitor(visitorID),
		museumID integer references museumschema.museum(museumID),
		visit_date date,
		primary key(visitorID,museumID)
);


create table if not exists museumschema.exhibition_item(
		exhibitionID integer references museumschema.exhibition(exhibitionID),
		itemID integer references museumschema.item(itemID),
		exhibition_date date check(exhibition_date<=current_date)
)



do $$
begin
   if not exists (select 1 from pg_constraint where conname = 'museum_name_checker') then
      alter table museumschema.museum
      add constraint museum_name_checker check (name is not null);
   end if;
end $$;


do $$
begin 
	if not exists(select 1 from pg_constraint where conname='storage_capacity_checker') then 
		alter table museumschema."storage" 
		add constraint storage_capacity_checker check(capacity>0);
	end if;
end $$;


do $$
begin 	
	if not exists(select 1 from pg_constraint where conname='employee_role_checker') then 
		alter table museumschema.employee 
		add constraint employee_role_checker check(upper(role) in('CURATOR','ARCHIVIST','GUIDE','CONSERVATOR',
		'EXHIBITION COORDINATOR','SECURITY OFFICER','RESEARCHER','EDUCATOR'));
	end if;
end $$;


do $$
begin
	if not exists(select 1 from pg_constraint where conname='visitor_email_checker') then 
		alter table museumschema.visitor 
		add constraint visitor_email_checker unique(email);
	end if;
end $$;



do $$
begin
	if not exists(select 1 from pg_constraint where conname='musum_visited_by_visitor_checker') then 
		alter table museumschema.museum_visitor 
		add constraint musum_visited_by_visitor_checker check(extract(year from visit_date)>2024 or 
		(extract(year from visit_date)=2024 and extract(month from visit_date)>7) or
		(extract(year from visit_date)=2024 and extract(month from visit_date)=7 and extract(day from visit_date)>1));
	end if;
end $$;







--inserting values into museum table

insert into museumschema.museum(name,location,open_date)
		select 'national art museum','new-york', date '1950-06-01'
		where not exists(
		select 1 
		from museumschema.museum m
		where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')
		)
		
		union all
		
		select 'national history museum','london',date '1881-04-18'
		where not exists(
		select 1 
		from museumschema.museum m
		where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		
		union all
		
		select 'museum of modern art','paris', date '1929-10-07'
		where not exists(
		select 1 
		from museumschema.museum m
		where upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris')
		)
		
		union all
		
		select 'science and technology museum','tokyo', date '1958-03-12'
		where not exists(
		select 1
		from museumschema.museum m
		where upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo'))
		
		union all
		
		select 'ancient civilization museum','cairo',date '1902-11-15'
		where not exists(
		select 1
		from museumschema.museum m
		where upper(m.name)=upper('ancient civilization museum') and upper(m.location)=upper('cairo'))
		
		union all 
		
		select 'space exploration museum','houston', date '1976-09-20'
		where not exists(
		select 1
		from museumschema.museum m
		where upper(m.name)=upper('space exploration museum') and upper(m.location)=upper('houston'))
		
		returning *;
	

--inserting values into employee table
	
insert into museumschema.employee(first_name,last_name,"role",museumid)
	select 'Emma','Wilson','curator',
				   (select m.museumid 
				   from museumschema.museum m
				   where upper(m."name")=upper('national art museum') and upper(m."location")=upper('new-york'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('emma') and upper(e.last_name)=upper('wilson') and upper(e.role)=upper('curator'))
	
			union all
			
	select 'james','anderson','archivist',
					(select m.museumid
					from museumschema.museum m
					where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('james') and upper(e.last_name)=upper('anderson') and upper(e.role)=upper('archivist'))
			
			union all
	
	select 'sophia','thomas','guide',
					(select  m.museumid
					from museumschema.museum m
					where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('sophia') and upper(e.last_name)=upper('thomas') and upper(e.role)=upper('guide'))
			
			union all
	
	select 	'liam','roberts','security officer',
			(select m.museumid 
			from museumschema.museum m
			where upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('liam') and upper(e.last_name)=upper('roberts') and upper(e.role)=upper('security officer'))
		
			union all
	
	select 'olivia','clark','conservator',
			(select m.museumid
			from museumschema.museum m
			where upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('olivia') and upper(e.last_name)=upper('clark') and upper(e.role)=upper('conservator'))
			
			union all
			
	select 'noah','harris','researcher',
			(select m.museumid
			from museumschema.museum m
			where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
			where not exists(
			select 1
			from museumschema.employee e
			where upper(e.first_name)=upper('noah') and upper(e.last_name)=upper('harris') and upper(e.role)=upper('researcher'))
			
		returning *;
			

--inserting values into table exhibition

insert into museumschema.exhibition(start_date,end_date,is_online,museumid)
		select date '2024-11-10', date '2024-11-12',false,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national art museum') and upper(m."location")=upper('new-york'))
		where not exists(
		select 1
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date= date '2024-11-10' and  e.end_date= date '2024-11-12' 
		and upper(m.name)=upper('national art museum') and upper(m."location")=upper('new-york'))
		
		union all
		
		select date '2024-10-01', date '2024-10-04',true,
			   (select m.museumid
			    from  museumschema.museum m
			    where upper(m.name)=upper('national art museum') and upper(m."location")=upper('new-york'))
		where not exists(
		select 1
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date=date '2024-10-01' and e.end_date=date '2024-10-04'
		and upper(m.name)=upper('national art museum') and upper(m."location")=upper('new-york'))
			
		union all
		
		select date	'2024-11-01',date '2024-11-05',false,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		where not exists(
		select 1 
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date=date '2024-11-01' and e.end_date= date '2024-11-05'
		and upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))

		union all
		
		select date '2024-10-15',date '2024-10-18',true,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris'))
		where not exists(
		select 1 
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date=date '2024-10-15' and e.end_date=date '2024-10-18'
		and upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris'))
		
		union all
		
		select date '2024-11-25',date '2024-11-30',false,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		where not exists(
		select 1
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date=date '2024-11-25' and e.end_date='2024-11-30'
		and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		
		union all
		
		select date '2024-10-20',date '2024-10-25',true,
			   (select m.museumid
			   from museumschema.museum m
			   where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		where not exists(
		select 1
		from museumschema.exhibition e
		join museumschema.museum m on e.museumid=m.museumid
		where e.start_date=date '2024-10-20' and e.end_date='2024-10-25'
		and upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		
		returning *;

	

	
	
--inserting into table visitor
insert into museumschema.visitor(first_name,last_name,email)

			select 'alice','jhonson','alice.johnson@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('alice.johnson@example.com'))
			
			union all
			
			select 'bob','smith','bob.smith@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('bob.smith@example.com'))
			
			union all
			
			select 'charlie','brown','charlie.brown@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('charlie.brown@example.com'))
			
			union all
			
			select 'diana','evans','diana.evans@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('diana.evans@example.com'))
			
			union all
			
			select 'ethan','martinez','ethan.martinez@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('ethan.martinez@example.com'))
			
			union all 
			
			select 'fiona','taylor','fiona.taylor@example.com'
			where not exists(
			select 1
			from museumschema.visitor v
			where upper(v.email)=upper('fiona.taylor@example.com'))
			
			returning *;
		

		
		
		
--inserting values into table storage
insert into  museumschema."storage"("location",capacity,museumid)
			
		select 'basement,wing A',500,
				(select m.museumid
				 from museumschema.museum m
				 where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		where not exists(
		select 1
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('basement,wing A') and 
		upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		
		union all
		
		select 'annex building room 3',300,
					(select m.museumid
				 from museumschema.museum m
				 where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		where not exists(
		select 1
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('annex building room 3') and 
		upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york'))
		
		union all
		
		select 'undenground vault',800,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		where not exists(
		select 1
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('undenground vault') and 
		upper(m.name)=upper('national history museum') and upper(m.location)=upper('london'))
		
		
		union all
		
		select 'main warehouse',1000,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris'))
		where not exists(
		select 1 
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('main warehouse') and 
		upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris'))
		
		union all
		
		select 'off-site facility 1',600,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo'))
		where not exists(
		select 1 
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('off-site facility 1') and 
		upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo'))
		
		union all
		
		select 'security archive room',200,
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('ancient civilization museum') and upper(m.location)=upper('cairo'))
		where not exists(
		select 1
		from museumschema."storage" s
		join museumschema.museum m on s.museumid=m.museumid
		where upper(s.location)=upper('security archive room') and 
		upper(m.name)=upper('ancient civilization museum') and upper(m.location)=upper('cairo'))
		
		returning *;
		


--inserting values into table item	
insert into  museumschema.item(storageid,category,description,name)

			select(select s.storageid 
				   from museumschema."storage" s
				   join museumschema.museum m on s.museumid=m.museumid
				   where upper(s.location)=upper('basement,wing A') 
				   and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			'artwork','  A Renaissance painting by Leonardo da Vinci, depicting a woman with a mysterious smile',
			'Mona-lisa'
			where not exists(
			select 1 
			from museumschema.item i
			where upper(i.category)=upper('artwork') and upper(i.name)=upper('mona-lisa'))
			
			union all
			
			select (select s.storageid 
				   from museumschema."storage" s
				   join museumschema.museum m on s.museumid=m.museumid
				   where upper(s.location)=upper('basement,wing A') 
				   and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			'historical object','An ancient Egyptian stone inscribed with three scripts that enabled the decoding of hieroglyphs.',
			'Rosetta Stone'
			where not exists(
			select 1
			from museumschema.item i
			where upper(i.category)=upper('historical object') and upper(i.name)=upper('rosetta stone'))
			
					
			union all
			
			select(select s.storageid
				   from museumschema."storage" s
				   join museumschema.museum m on s.museumid=m.museumid
				   where upper(s.location)=upper('annex building room 3')
				   and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			'artifact','A clay vase from the 5th century BCE featuring black-figure pottery artwork.',
			'ancient greek vase'
			where not exists(
			select 1
			from museumschema.item i
			where upper(i.category)=upper('artifact') and upper(i.name)=upper('ancient greek vase'))
			
			union all
			
			select (select s.storageid
					from museumschema."storage" s
					join museumschema.museum m on s.museumid=m.museumid
					where upper(s.location)=upper('security archive room')
					and upper(m.name)=upper('ancient civilization museum') and upper(m.location)=upper('cairo')),
			'historical object','A 12th-century knights sword, crafted with ornate engravings',
			'medieval sword'
			where not exists(
			select 1
			from museumschema.item i
			where upper(i.category)=upper('historical object') and upper(i.name)=upper('medieval sword'))
			
			union all
			
			select (select s.storageid
					from museumschema."storage" s
					join museumschema.museum m on s.museumid=m.museumid
					where upper(s.location)=upper('undenground vault')
					and upper(m.name)=upper('national history museum') and upper(m.location)=upper('london')),
			'artifact','An ancient Egyptian stone sculpture of a pharaoh','egyptian sculpture'
			where not exists(
			select 1 
			from museumschema.item i
			where upper(i.category)=upper('artifact') and upper(i.name)=upper('egyptian sculpture'))
			
			union all
			
			select (select s.storageid
					from museumschema."storage" s
					join museumschema.museum m on s.museumid=m.museumid
					where upper(s.location)=upper('main warehouse') and 
					upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris')),
			'artifact','A beautifully painted vase from the Renaissance period with floral motifs','Renaissance Vase'
			where not exists(
			select 1
			from museumschema.item i
			where upper(i.category)=upper('artifact') and upper(i.name)=upper('renaissance vase'))
			
			returning *;
					
	 
--insert values into table museum_visitor
insert into museumschema.museum_visitor(visitorid,museumid,visit_date)
		
		select (select v.visitorid
				from museumschema.visitor v
				where upper(v.email)=upper('alice.johnson@example.com')),
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
				date '2024-11-25'
		where not exists(
		select 1 
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('alice.johnson@example.com') and
		upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york') and
		mv.visit_date=date '2024-11-25')
		
		union all
		
		select (select v.visitorid
				from museumschema.visitor v
				where upper(v.email)=upper('bob.smith@example.com')),
			   (select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
				date '2024-10-01'
		where not exists(
		select 1
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('bob.smith@example.com') and
		upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york') and
		mv.visit_date=date '2024-10-01')
		
		union all
		
		select (select v.visitorid
				from museumschema.visitor v
				where upper(v.email)=upper('charlie.brown@example.com')),
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london')),
				date '2024-10-16'
		where not exists(
		select 1
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('charlie.brown@example.com') and 
		upper(m.name)=upper('national history museum') and upper(m.location)=upper('london') and
		mv.visit_date=date '2024-10-16'
		)
		
		union all
		
		select (select v.visitorid
				from museumschema.visitor v
				where upper(v.email)=upper('diana.evans@example.com')),
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris')),
				date '2024-11-26'
		where not exists(
		select 1
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('diana.evans@example.com') and 
		upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris') and
		mv.visit_date=date '2024-11-26'
		)
		
		union all 
		
		select (select v.visitorid
				from museumschema.visitor v
				where upper(v.email)=upper('ethan.martinez@example.com')),
				(select m.museumid
				from museumschema.museum m
				where upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo')),
				date '2024-10-22'
		where not exists(
		select 1
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('ethan.martinez@example.com') and 
		upper(m.name)=upper('science and technology museum') and upper(m.location)=upper('tokyo') and 
		mv.visit_date= date '2024-10-22'
		)
		
		union all
		
		select (select v.visitorid
				from museumschema.visitor v 
				where upper(v.email)=upper('fiona.taylor@example.com')),
			   (select m.museumid
			   from	museumschema.museum m
			   where upper(m.name)=upper('national history museum') and upper(m.location)=upper('london')),
			   date '2024-10-23'
		where not exists(
		select 1
		from museumschema.museum_visitor mv
		join museumschema.museum m on mv.museumid=m.museumid
		join museumschema.visitor v on mv.visitorid=v.visitorid
		where upper(v.email)=upper('fiona.taylor@example.com') and 
		upper(m.name)=upper('national history museum') and upper(m.location)=upper('london') and 
		mv.visit_date= date '2024-10-23'
		)
		
		returning *;
	

	
--inserting values into table exhibition_item

insert into museumschema.exhibition_item(exhibitionid,itemid,exhibition_date)
		
		select (select e.exhibitionid
				from museumschema.exhibition e
				join museumschema.museum m on e.museumid=m.museumid
				where e.start_date=date '2024-11-25' and e.end_date='2024-11-30'
				and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			   (select i.itemid
			   from museumschema.item i
			   where upper(i.category)=upper('artwork') and upper(i.name)=upper('mona-lisa')),
			   date '2024-11-25'
		where not exists(
		select 1 
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date=date '2024-11-25' and e.end_date='2024-11-30'
			and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york') and
			upper(i.category)=upper('artwork') and upper(i.name)=upper('mona-lisa') and 
			ei.exhibition_date=date '2024-11-25')
			
		union all
		
		select(select e.exhibitionid
			   from museumschema.exhibition e
			   join museumschema.museum m on e.museumid=m.museumid
			   where e.start_date= date '2024-10-01' and e.end_date='2024-10-04'
			   and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			  (select i.itemid
			  from museumschema.item i
			  where upper(i.category)=upper('historical object') and upper(i.name)=upper('rosetta stone')),
			  date '2024-10-01'
		where not exists(
		select 1
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date= date '2024-10-01' and e.end_date='2024-10-04'
			  and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york') and
			  upper(i.category)=upper('historical object') and upper(i.name)=upper('rosetta stone') and 
			  ei.exhibition_date=date '2024-10-01'
		)
		
		union all 
		
		select (select e.exhibitionid
				from museumschema.exhibition e
				join museumschema.museum m on e.museumid=m.museumid
				where e.start_date= date '2024-10-15' and e.end_date='2024-10-18' and
				upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris')),
			   (select i.itemid
			   from museumschema.item i
			   where upper(i.category)=upper('artifact') and upper(i.name)=upper('ancient greek vase')),
			   date '2024-10-16'
		where not exists(
		select 1
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date= date '2024-10-15' and e.end_date='2024-10-18' and
			upper(m.name)=upper('museum of modern art') and upper(m.location)=upper('paris') and 
			upper(i.category)=upper('artifact') and upper(i.name)=upper('ancient greek vase') and 
			ei.exhibition_date=date '2024-10-16'
		)
		
		union all
		
		select (select e.exhibitionid
				from museumschema.exhibition e
				join museumschema.museum m on e.museumid=m.museumid
				where e.start_date=date '2024-11-25' and e.end_date='2024-11-30'
				and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york')),
			   (select i.itemid
			   from museumschema.item i
			   where upper(i.category)=upper('historical object') and upper(i.name)=upper('medieval sword')),
			   date '2024-11-26'
		where not exists(
		select 1
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date=date '2024-11-25' and e.end_date='2024-11-30'
			and upper(m.name)=upper('national art museum') and upper(m.location)=upper('new-york') and 
			upper(i.category)=upper('historical object') and upper(i.name)=upper('medieval sword') and 
			ei.exhibition_date=date '2024-11-26'
		)
		
		union all
		
		
		select (select e.exhibitionid
				from museumschema.exhibition e
				join museumschema.museum m on e.museumid=m.museumid
				where e.start_date=date '2024-10-20' and e.end_date= date '2024-10-25' and
				upper(m.name)=upper('national history museum') and upper(m.location)=upper('london')),
			   (select i.itemid
			   from museumschema.item i
			   where upper(i.category)=upper('artifact') and upper(i.name)=upper('egyptian sculpture')),
			   date '2024-10-22'
		where not exists(
		select 1
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date=date '2024-10-20' and e.end_date= date '2024-10-25' and
			 upper(m.name)=upper('national history museum') and upper(m.location)=upper('london') and 
			 upper(i.category)=upper('artifact') and upper(i.name)=upper('egyptian sculpture') and 
			 ei.exhibition_date=date '2024-10-22'
		)
		
		union all
		
		
		select (select e.exhibitionid
				from museumschema.exhibition e
				join museumschema.museum m on e.museumid=m.museumid
				where e.start_date=date '2024-10-20' and e.end_date= date '2024-10-25' and
				upper(m.name)=upper('national history museum') and upper(m.location)=upper('london')),
			   (select i.itemid
			   from museumschema.item i
			   where upper(i.category)=upper('historical object') and upper(i.name)=upper('medieval sword')),
			   date '2024-10-23'
		where not exists(
		select 1
		from museumschema.exhibition_item ei
		join museumschema.exhibition e on e.exhibitionid=ei.exhibitionid
		join museumschema.museum m on e.museumid=e.museumid
		join museumschema.item i on ei.itemid=i.itemid
		where e.start_date=date '2024-10-20' and e.end_date= date '2024-10-25' and
			upper(m.name)=upper('national history museum') and upper(m.location)=upper('london') and 
			upper(i.category)=upper('historical object') and upper(i.name)=upper('medieval sword') and 
			ei.exhibition_date=date '2024-10-23'
		)
		
		returning *;
			   
		
--creating function to update employee table

create or replace function museumschema.modify_employee_name(
			employees_primary_key int,
			column_name text,
			new_value text
	)
	
returns void as $$
declare
	query text;
begin
		query:=format(
		'update museumschema.employee set %I =$1 where employeeid=$2',
		column_name  );
	
	execute query using new_value,employees_primary_key;

end; $$
language plpgsql;


--task 5.2

create or replace function museumschema.insert_visit(
		visitor_id int,
		museum_id int,
		visitdate date
)
returns text 
language plpgsql
as $$
begin 
		if visitdate>current_date then 
		return  ('this transaction is not possible');
		end if;
	
	insert into museumschema.museum_visitor(visitorid,museumid,visit_date)
			
			select visitor_id,museum_id,visitdate;
		
		return 'Transaction was successful';
			
end; $$;

/* This table was the closest to being a transaction table, so instead of creating a new table 
 * I thought it would be a better idea to use this table for this task.
 * 
 */




--creating the view   
create or replace view museumschema.analytics_recent_quarter as
with recent_quarter as (
    select 
        to_char(visit_date, 'yyyy-"q"q') as quarter
    from museumschema.museum_visitor
    order by visit_date desc
    limit 1
)
select 
    rq.quarter as quarter,
    m.name as museum_name,
    count(distinct v.visitorid) as total_visitors,
    count(distinct e.exhibitionid) as total_exhibitions,
    count(distinct case when e.is_online then e.exhibitionid end) as online_exhibitions,
    count(distinct case when not e.is_online then e.exhibitionid end) as onsite_exhibitions
from museumschema.visitor v
join museumschema.museum_visitor mv  on v.visitorid = mv.visitorid
join museumschema.museum m on m.museumid = mv.museumid
left join museumschema.exhibition e on m.museumid = e.museumid
cross join recent_quarter rq
where 
    to_char(mv.visit_date, 'yyyy-"q"q') = rq.quarter
group by 
    rq.quarter, m.name;

/* I created this view to keep track the most resent visitors visit_dates for the museums. */







--creating the manager role
do $$
begin
    begin
        create role manager login;
    exception
        when duplicate_object then
            null; -- if the role already exists, do nothing
    end;
end $$;


grant connect on database  museum to manager;
grant usage on schema museumschema to manager;
grant select on all tables in schema museumschema to manager;


