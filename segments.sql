
--1. get timestamps of each segment
-- segment 1 is from start to first tower kill
drop table if exists segment_timestamp;
create temp table segment_timestamp as (
select matchid, timestamp 
from (
	select 
		matchid, 
		timestamp, 
		row_number() over (partition by matchid order by timestamp) as segment
	from event 
	--where matchid = 145226393 
	where eventtype in ('BUILDING_KILL') 
	) a
where segment = 1
);
--select * from segment_timestamp;

--2. create table of segment features - minions killed, level, gold, xp
drop table if exists segment_features;
create table segment_features as (
select 
	matchid, 
	timestamp, 
	sum(jungleminionskilled) as jungleminionskilled,
	sum(minionskilled) as minionskilled,
	sum(level) as level, 
	sum(totalgold) as totalgold,
	sum(xp) as xp	
from (
	select 
		a.matchid, 
		a.timestamp, 
		a.level, 
		a.jungleminionskilled,
		a.minionskilled,
		a.totalgold,
		a.xp,
		max(a.timestamp) over (partition by a.matchid) as max_timestamp
	from participant_frame a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp <= b.timestamp
	) a
where timestamp = max_timestamp
group by 1,2
); 

--select * from segment_features limit 1000; 

--3. add to segment features - building kills
drop table if exists building_kills;
create temp table building_kills as (
select 
	matchid, 
	timestamp, 
	sum(building_kills) as building_kills, 
	sum(inhibitor_kills) as inhibitor_kills, 
	sum(nexus_kills) as nexus_kills
from (
	select 
		a.matchid, 
		--eventtype, buildingtype, lanetype, towertype,
		a.timestamp,
		count(1) as building_kills,
		sum(case when a.buildingtype = 'INHIBITOR_BUILDING' then 1 else 0 end) as inhibitor_kills,
		sum(case when a.towertype = 'NEXUS_TURRET' then 1 else 0 end) as nexus_kills,
		max(a.timestamp) over (partition by a.matchid) as max_timestamp
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp <= b.timestamp
	where a.eventtype in ('BUILDING_KILL')
	group by 1,2
	) a
where timestamp = max_timestamp
group by 1,2
);
select * from building_kills limit 1000;

--4. add to segment features - champion kills
drop table if exists champion_kills;
create temp table champion_kills as (
select 
	matchid, 
	timestamp, 
	sum(champion_kills) as champion_kills
from (
	select 
		a.matchid,  
		a.timestamp,
		count(1) as champion_kills,
		max(a.timestamp) over (partition by a.matchid) as max_timestamp
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp <= b.timestamp
	where a.eventtype in ('CHAMPION_KILL')
	group by 1,2
	) a
where timestamp = max_timestamp
group by 1,2
);
select * from champion_kills limit 1000;

--5. add to segment features - monster kills
drop table if exists monster_kills;
create temp table monster_kills as (
select 
	matchid, 
	timestamp, 
	sum(monster_kills) as monster_kills
from (
	select 
		a.matchid,  
		a.timestamp,
		sum(case when a.monstertype = 'BARON_NASHOR' then 1 else 0 end) as baron_kills, 
		sum(case when a.monstertype = 'DRAGON' then 1 else 0 end) as dragon_kills,
		max(a.timestamp) over (partition by a.matchid) as max_timestamp
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp <= b.timestamp
	where a.eventtype in ('ELITE_MONSTER_KILL')
	group by 1,2
	) a
where timestamp = max_timestamp
group by 1,2
);
select * from monster_kills limit 1000;

--6. add to segment features - items purchased
drop table if exists items_purchased;
create temp table items_purchased as (
select 
	matchid, 
	timestamp, 
	sum(items_purchased) as items_purchased
from (
	select 
		a.matchid,  
		a.timestamp,
		count(1) as items_purchased,
		max(a.timestamp) over (partition by a.matchid) as max_timestamp
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp <= b.timestamp
	where a.eventtype in ('ITEM_PURCHASED')
	group by 1,2
	) a
where timestamp = max_timestamp
group by 1,2
);
select * from items_purchased limit 1000;


--copy segment_features to 'segment_features.csv' delimiter ',' csv header;

