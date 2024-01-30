use master
go

drop database leiden_ranking_open_edition_2023
go

create database leiden_ranking_open_edition_2023
	on (name=leiden_ranking_open_edition_2023, filename='I:\MSSQL\data\leiden_ranking_open_edition_2023.mdf')
	log on (name=leiden_ranking_open_edition_2023_log, filename='L:\MSSQL\log\leiden_ranking_open_edition_2023_log.ldf')
go

alter database leiden_ranking_open_edition_2023 set recovery simple
go
