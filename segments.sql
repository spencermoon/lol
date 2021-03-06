/*
select eventtype, towertype, avg(timestamp) as avg_time
from event
where eventtype in ('BUILDING_KILL') and buildingtype in ('TOWER_BUILDING')
group by 1,2
order by 3;
*/
--60 s: 640,047
--30 s: 691,518
--15 s: 704,826


select count(1) from (
select  
	matchid,  
	'Z_TEAM_FIGHT_' || row_number() over (partition by matchid order by timestamp) as segment,
	case when (timestamp - 1.5*60*1000) < 0 then 0 
		else (timestamp - 1.5*60*1000) 
		end as start_time,
	timestamp + 1.5*60*1000 as end_time
from (
select 
	*, 
	lag(rn) over (partition by matchid order by timestamp) as rn_lag, 
	rn - lag(rn) over (partition by matchid order by timestamp) as rn_flag
from (
select 
	*, 
	((x-x2)^2 + (y-y2)^2)^.5 as distance
from (
	select 
		*, 
		timestamp - lag(timestamp) over (partition by matchid order by timestamp) as time_between_kills, 
		lag(x) over (partition by matchid order by timestamp) as x2, 
		lag(y) over (partition by matchid order by timestamp) as y2
	from (
		select 
			matchid, 
			timestamp as timestamp, 
			position::varchar,
			(split_part(split_part(position::varchar, ',', 1), '(', 2))::int as x, 
			(split_part(split_part(position::varchar, ',', 2), ')', 1))::int as y,
			row_number() over (partition by matchid order by timestamp) as rn
		from event 
		--where matchid = 145226393
		where eventtype = 'CHAMPION_KILL'
		order by timestamp
		) a
	) b 
where (((x-x2)^2 + (y-y2)^2)^.5) < 5000
and time_between_kills <= 60*1000
) c
) d 
where rn_flag > 1 
	) e
;


--1a. get timestamps of each segment except champion kills
drop table if exists segment_timestamp_1;
create temp table segment_timestamp_1 as (
select 
	matchid,  
	case when coalesce(monstertype, towertype, eventtype) = 'UNDEFINED_TURRET' THEN 'INHIBITOR' 
		ELSE coalesce(monstertype, towertype, eventtype) 
		end as segment,
	case when (timestamp - 1.5*60*1000) < 0 then 0 
		else (timestamp - 1.5*60*1000) 
		end as start_time,
	timestamp + 1.5*60*1000 as end_time
from (
	select 
		matchid, 
		eventtype::text,
		towertype::text, 
		buildingtype::text,
		monstertype::text,
		timestamp,
		row_number() over (partition by matchid, eventtype, towertype, buildingtype, monstertype order by timestamp) as rn
	from event 
	--where matchid = 145226393 
	where eventtype in ('BUILDING_KILL', 'CHAMPION_KILL', 'ELITE_MONSTER_KILL') 
	) a
where rn = 1
);
select * from segment_timestamp_1 limit 1000;


--1b. get timestamps of just champion kills
drop table if exists segment_timestamp_2;
create temp table segment_timestamp_2 as (
select 
	matchid,  
	'Z_TEAM_FIGHT_' || row_number() over (partition by matchid order by timestamp) as segment,
	case when (timestamp - 1.5*60*1000) < 0 then 0 
		else (timestamp - 1.5*60*1000) 
		end as start_time,
	timestamp + 1.5*60*1000 as end_time
from (
select 
	*, 
	lag(rn) over (partition by matchid order by timestamp) as rn_lag, 
	rn - lag(rn) over (partition by matchid order by timestamp) as rn_flag
from (
select 
	*, 
	((x-x2)^2 + (y-y2)^2)^.5 as distance
from (
	select 
		*, 
		timestamp - lag(timestamp) over (partition by matchid order by timestamp) as time_between_kills, 
		lag(x) over (partition by matchid order by timestamp) as x2, 
		lag(y) over (partition by matchid order by timestamp) as y2
	from (
		select 
			matchid, 
			timestamp as timestamp, 
			position::varchar,
			(split_part(split_part(position::varchar, ',', 1), '(', 2))::int as x, 
			(split_part(split_part(position::varchar, ',', 2), ')', 1))::int as y,
			row_number() over (partition by matchid order by timestamp) as rn
		from event 
		--where matchid < 145226393
		where eventtype = 'CHAMPION_KILL'
		order by timestamp
		) a
	) b 
where (((x-x2)^2 + (y-y2)^2)^.5) < 5000
and time_between_kills <= 60*1000
) c
) d 
where rn_flag > 1 
);
select * from segment_timestamp_2 limit 1000;


--1c. union segment timestamps
drop table if exists segment_timestamp;
create temp table segment_timestamp as (
select * from segment_timestamp_1 
	union all 
select * from segment_timestamp_2 	
);

select *, (start_time/60000)::int as start_min from segment_timestamp where matchid = 145226393 order by start_time limit 1000;


--2a. create table of segment features - minions killed, level, gold, xp
drop table if exists gold_xp_etc_max;
create temp table gold_xp_etc_max as (
select 
	matchid, 
	segment, 
	start_time,
	end_time,
	timestamp, 
	sum(jungleminionskilled) as jungleminionskilled,
	sum(minionskilled) as minionskilled,
	sum(level) as level, 
	sum(totalgold) as totalgold,
	sum(xp) as xp--,
	--count(1)
from (
	select 
		a.matchid, 
		b.segment, 
		b.start_time,
		b.end_time,
		a.timestamp, 
		a.level, 
		a.jungleminionskilled,
		a.minionskilled,
		a.totalgold,
		a.xp,
		max(a.timestamp) over (partition by a.matchid, b.segment) as max_timestamp --use only data for latest timestamp to not duplicate data
	from participant_frame a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	--order by 2,5,11
	) a
where timestamp = max_timestamp
group by 1,2,3,4,5
); 

--2b. create table of segment features - minions killed, level, gold, xp
drop table if exists gold_xp_etc_min;
create temp table gold_xp_etc_min as (
select 
	matchid, 
	segment, 
	start_time,
	end_time,
	timestamp, 
	sum(jungleminionskilled) as jungleminionskilled,
	sum(minionskilled) as minionskilled,
	sum(level) as level, 
	sum(totalgold) as totalgold,
	sum(xp) as xp--,
	--count(1)
from (
	select 
		a.matchid, 
		b.segment, 
		b.start_time,
		b.end_time,
		a.timestamp, 
		a.level, 
		a.jungleminionskilled,
		a.minionskilled,
		a.totalgold,
		a.xp,
		min(a.timestamp) over (partition by a.matchid, b.segment) as min_timestamp --use only data for latest timestamp to not duplicate data
	from participant_frame a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	--order by 2,5,11
	) a
where timestamp = min_timestamp
group by 1,2,3,4,5
); 

--2c. create table of segment features - minions killed, level, gold, xp
drop table if exists gold_xp_etc;
create temp table gold_xp_etc as (
select 
	matchid, 
	segment, 
	start_time,
	end_time,
	a.jungleminionskilled - b.jungleminionskilled as jungleminionskilled,
	a.minionskilled - b.minionskilled as minionskilled,
	a.level - b.level as level, 
	a.totalgold - b.totalgold as totalgold,
	a.xp - b.xp as xp
from gold_xp_etc_max a
join gold_xp_etc_min b
using (matchid, segment, start_time, end_time)
); 
select * from gold_xp_etc limit 1000; 
select *, (start_time/60000)::int as start_min from gold_xp_etc where matchid = 145226393 order by start_time limit 1000;



--3. add to segment features - building kills
drop table if exists building_kills;
create temp table building_kills as (
	select 
		a.matchid, 
		b.segment, 
		b.start_time,
		b.end_time,
		--eventtype, buildingtype, lanetype, towertype,
		sum(case when a.buildingtype <> 'INHIBITOR_BUILDING' AND a.towertype <> 'NEXUS_TURRET' then 1 else 0 end) as building_kills,
		sum(case when a.buildingtype = 'INHIBITOR_BUILDING' then 1 else 0 end) as inhibitor_kills,
		sum(case when a.towertype = 'NEXUS_TURRET' then 1 else 0 end) as nexus_kills
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	where a.eventtype in ('BUILDING_KILL')
	group by 1,2,3,4
);
select * from building_kills limit 1000;

--4. add to segment features - champion kills
drop table if exists champion_kills;
create temp table champion_kills as (
	select 
		a.matchid, 
		b.segment, 
		b.start_time,
		b.end_time, 
		--a.timestamp,
		count(1) as champion_kills
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	where a.eventtype in ('CHAMPION_KILL')
	group by 1,2,3,4
);
select * from champion_kills limit 1000;

--5. add to segment features - monster kills
drop table if exists monster_kills;
create temp table monster_kills as (
	select 
		a.matchid,  
		b.segment, 
		b.start_time,
		b.end_time,
		--a.timestamp,
		sum(case when a.monstertype = 'BARON_NASHOR' then 1 else 0 end) as baron_kills, 
		sum(case when a.monstertype = 'DRAGON' then 1 else 0 end) as dragon_kills,
		sum(case when a.monstertype = 'RIFTHERALD' then 1 else 0 end) as rift_kills
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	where a.eventtype in ('ELITE_MONSTER_KILL')
	group by 1,2,3,4
);
select * from monster_kills limit 1000;

--6. add to segment features - items purchased
drop table if exists items_purchased;
create temp table items_purchased as (
	select 
		a.matchid,  
		b.segment, 
		b.start_time,
		b.end_time,
		count(1) as items_purchased
	from event a
	inner join segment_timestamp b
	on a.matchid = b.matchid
	and a.timestamp > b.start_time
	and a.timestamp <= b.end_time
	where a.eventtype in ('ITEM_PURCHASED')
	group by 1,2,3,4
);
select * from items_purchased limit 1000;


--copy segment_features to 'segment_features.csv' delimiter ',' csv header;

--7. join all segment features in perm table
drop table if exists segment_features2;
create table segment_features2 as (
select 
	a.matchid, 
	a.segment,
	(a.start_time + a.end_time)/2 as timestamp,
	coalesce(b.jungleminionskilled, 0) as jungleminionskilled,
	coalesce(b.minionskilled, 0) as minionskilled,
	coalesce(b.level, 0) as level,
	coalesce(b.totalgold, 0) as totalgold,
	coalesce(b.xp, 0) as xp,
	coalesce(c.building_kills, 0) as building_kills,
	coalesce(c.inhibitor_kills, 0) as inhibitor_kills,
	coalesce(c.nexus_kills, 0) as nexus_kills,
	coalesce(d.champion_kills, 0) as champion_kills,
	coalesce(e.baron_kills, 0) as baron_kills,
	coalesce(e.dragon_kills, 0) as dragon_kills,
	coalesce(e.rift_kills, 0) as rift_kills,
	coalesce(f.items_purchased, 0) as items_purchased
from segment_timestamp a 
left join gold_xp_etc b 
on a.matchid = b.matchid
and a.segment = b.segment
left join building_kills c 
on a.matchid = c.matchid
and a.segment = c.segment
left join champion_kills d 
on a.matchid = d.matchid
and a.segment = d.segment
left join monster_kills e 
on a.matchid = e.matchid
and a.segment = e.segment
left join items_purchased f 
on a.matchid = f.matchid
and a.segment = f.segment
	
);
grant all on segment_features2 to public;
select * from segment_features2 limit 1000;

--261,981 rows
--1,053,680 rows
select count(1) from segment_features2; 

--min gets A, max gets Z
--select min(segment) from segment_timestamp_1;

select * from segment_features2 where matchid = 92352108 order by 3;


