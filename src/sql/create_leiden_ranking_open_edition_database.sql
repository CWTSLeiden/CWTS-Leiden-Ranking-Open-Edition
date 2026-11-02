use master
go

drop database leiden_ranking_open_edition_2026
go

create database leiden_ranking_open_edition_2026
	on (name=leiden_ranking_open_edition_2026, filename='H:\MSSQL\data\leiden_ranking_open_edition_2026.mdf')
	log on (name=leiden_ranking_open_edition_2026_log, filename='L:\MSSQL\log\leiden_ranking_open_edition_2026_log.ldf')
go

alter database leiden_ranking_open_edition_2026 set recovery simple
go
