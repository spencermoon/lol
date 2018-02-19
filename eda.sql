--banning players - not important to us
select * from ban limit 1000;

--doesn't have timestamp info - not important maybe
select * from mastery limit 1000;

--names - not important
select * from player limit 1000;

--rune - not important
select * from rune limit 1000;

--aggregated by match - who got first baron, who won etc
select * from team limit 1000;

--empty table
select * from participant_identity limit 1000; 
 
----------------------------------------------------------------------

--has match info - filter out non classic matchmode, season etc, filter where frameinterval is not null
select * from match limit 1000;

--for each match - rank, participantstats to parse out
select * from participant limit 1000; 

--frame has current gold, aggregate gold, aggregate minions killed by time, position, level
--xp info
select * from participant_frame where matchid = 145226393 and participantid = 4 order by timestamp desc limit 1000; 

--has timestamp
--building_kill has killerid but participantid is 0
select * from event where eventtype in ('BUILDING_KILL') limit 1000;

--has position 
--teamid is 0
select * from event where eventtype in ('CHAMPION_KILL') limit 1000;

select * from event where matchid = 145226393 order by timestamp limit 1000;

select 
	eventtype,  
	ascendedtype, 
	buildingtype, 
	lanetype, 
	leveluptype,
	monstertype, 
	towertype,
	wardtype,
	count(1), 
	min(timestamp),
	max(timestamp)
from event 
where matchid = 144755111 
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8;

--just has eventid and assistingparticipantid - players that assist 
select * from event_assisting limit 1000; 
select * from event where eventid = 297;







