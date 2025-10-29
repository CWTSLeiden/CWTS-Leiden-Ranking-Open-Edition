use master
go

drop database leiden_ranking_open_edition_2025
go

create database leiden_ranking_open_edition_2025
	on (name=leiden_ranking_open_edition_2025, filename='H:\MSSQL\data\leiden_ranking_open_edition_2025.mdf')
	log on (name=leiden_ranking_open_edition_2025_log, filename='L:\MSSQL\log\leiden_ranking_open_edition_2025_log.ldf')
go

alter database leiden_ranking_open_edition_2025 set recovery simple
go
