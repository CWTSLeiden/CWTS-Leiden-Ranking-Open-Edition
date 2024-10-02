use master
go

drop database leiden_ranking_open_edition_2024
go

create database leiden_ranking_open_edition_2024
	on (name=leiden_ranking_open_edition_2024, filename='G:\MSSQL\data\leiden_ranking_open_edition_2024.mdf')
	log on (name=leiden_ranking_open_edition_2024_log, filename='L:\MSSQL\log\leiden_ranking_open_edition_2024_log.ldf')
go

alter database leiden_ranking_open_edition_2024 set recovery simple
go
