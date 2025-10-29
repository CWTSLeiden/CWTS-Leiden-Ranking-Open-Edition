use leiden_ranking_open_edition_2025
go

declare @period_n_years int = 4
declare @first_period_begin_year int = 2006
declare @last_period_begin_year int = 2020
declare @last_period_end_year int = @last_period_begin_year + @period_n_years - 1



-- Create period table. Each row in this table represents a time period.

drop table if exists [period]
create table [period]
(
	period_begin_year smallint not null,
	period_end_year smallint not null,
	[period] varchar(15) not null
)

declare @period_begin_year int = @first_period_begin_year
while @period_begin_year <= @last_period_begin_year
begin
	declare @period_end_year int = @period_begin_year + @period_n_years - 1
	insert into [period] values (@period_begin_year, @period_end_year, cast(@period_begin_year as varchar(50)) + '-' + cast(@period_end_year as varchar(50)))
	set @period_begin_year += 1
end

alter table [period] add constraint pk_period primary key(period_begin_year)



-- Create main_field table. Each row in this table represents a main field.

drop table if exists main_field
create table main_field
(
	main_field_id tinyint not null,
	main_field varchar(50) not null
)

insert into main_field with(tablock)
select 0, 'All sciences'

insert into main_field with(tablock)
select main_field_id, main_field
from openalex_2025aug_classification..main_field

alter table main_field add constraint pk_main_field primary key(main_field_id)



-- Create publication table. Each row in this table represents a publication.
-- Only articles, reviews, and book chapters published in journals, book series, standalone books, and conference proceedings are included.
-- Publications must not be retracted and must contain sufficient metadata (i.e., source, authors, affiliations, references).

drop table if exists #pub
select a.work_id, a.pub_year, is_core_pub = b.is_core_work, micro_cluster_id = d.research_area_no
into #pub
from openalex_2025aug..work as a
join openalex_2025aug_core..work as b on a.work_id = b.work_id
join openalex_2025aug_indicators..pub as c on a.work_id = c.work_id
join openalex_2025aug_indicators..pub_classification_system_research_area as d on a.work_id = d.work_id
where a.pub_year between @first_period_begin_year and @last_period_end_year
	and c.doc_type_no <> 1
	and d.classification_system_no = 2

drop table if exists pub
create table pub
(
	work_id bigint not null,
	pub_year smallint not null,
	is_core_pub bit not null
)

insert into pub with(tablock)
select work_id, pub_year, is_core_pub
from #pub

alter table pub add constraint pk_pub primary key(work_id)
create index idx_pub_pub_year on pub(pub_year)
create index idx_pub_is_core_pub on pub(is_core_pub)



-- Create pub_main_field table. This table links publications to main fields.

drop table if exists pub_main_field
create table pub_main_field
(
	work_id bigint not null,
	main_field_id tinyint not null,
	[weight] float not null
)

insert into pub_main_field with(tablock)
select work_id, main_field_id = 0, [weight] = 1
from #pub
union
select a.work_id, main_field_id = b.main_field_id, b.[weight]
from #pub as a
join openalex_2025aug_classification..micro_cluster_main_field as b on a.micro_cluster_id = b.micro_cluster_id

alter table pub_main_field add constraint pk_pub_main_field primary key(work_id, main_field_id)



-- Indentify candidate universities.

drop table if exists #institution_ror_id
select a.institution_id, a.ror_id
into #institution_ror_id
from openalex_2025aug..institution as a
where a.ror_id is not null

delete from a
from #institution_ror_id as a
join
(
	select ror_id, n = count(*)
	from #institution_ror_id
	group by ror_id
	having count(*) > 1
) as b on a.ror_id = b.ror_id
left join
(
	select distinct institution_id
	from openalex_2025aug..work_affiliation_institution
) as c on a.institution_id = c.institution_id
where c.institution_id is null

drop table if exists #university
select a.university_id, a.university_ror_id, university_openalex_institution_id = b.institution_id
into #university
from
(
	select distinct university_id = cwts_organization_id, university_ror_id = ror_id
	from projectdb_leiden_ranking_open_edition..LROE2025_university_affiliated_organization_20251024
) as a
join #institution_ror_id as b on a.university_ror_id = b.ror_id



-- Create university_affiliated_organization table. Each row in this table represents the relation between a university and an affiliated organization.

drop table if exists university_affiliated_organization
create table university_affiliated_organization
(
	university_ror_id char(9) not null,
	university_openalex_institution_id bigint not null,
	relation_type varchar(10) not null,
	affiliated_organization_ror_id char(9) not null,
	affiliated_organization_openalex_institution_id bigint not null,
	affiliated_organization_weight float null
)

insert into university_affiliated_organization with(tablock)
select distinct
	university_ror_id = a.ror_id,
	university_openalex_institution_id = b.institution_id,
	relation_type = a.relation_type,
	affiliated_organization_ror_id = a.affiliated_organization_ror_id,
	affiliated_organization_openalex_institution_id = c.institution_id,
	affiliated_organization_weight = a.affiliated_organization_weight
from projectdb_leiden_ranking_open_edition..LROE2025_university_affiliated_organization_20251024 as a
join #institution_ror_id as b on a.ror_id = b.ror_id
join #institution_ror_id as c on a.affiliated_organization_ror_id = c.ror_id
where a.ror_id <> a.affiliated_organization_ror_id

alter table university_affiliated_organization add constraint pk_university_affiliated_organization primary key(university_ror_id, affiliated_organization_ror_id)



-- Create affiliated_organization table. Each row in this table represents an affiliated (component, joint, or associated) organization.

drop table if exists affiliated_organization
create table affiliated_organization
(
	affiliated_organization_ror_id char(9) not null,
	affiliated_organization_openalex_institution_id bigint not null,
	affiliated_organization_ror_name varchar(150) not null
)

insert into affiliated_organization with(tablock)
select a.affiliated_organization_ror_id, a.affiliated_organization_openalex_institution_id, b.organization
from
(
	select distinct affiliated_organization_ror_id, affiliated_organization_openalex_institution_id
	from university_affiliated_organization
) as a
left join ror_2025aug..organization as b on a.affiliated_organization_ror_id = b.ror_id

alter table affiliated_organization add constraint pk_affiliated_organization primary key(affiliated_organization_ror_id)



-- Link publications to institutions and calculate the weight of linked institutions based on the number of affiliated authors.

drop table if exists #pub_n_authors
select a.work_id, n_authors = count(*)
into #pub_n_authors
from openalex_2025aug..work_author as a 
join pub as b on a.work_id = b.work_id
group by a.work_id

drop table if exists #pub_author_affiliation
select a.work_id, a.author_seq, a.affiliation_seq, [weight] = cast(1 as float) / (b.n_authors * count(*) over (partition by a.work_id, a.author_seq))
into #pub_author_affiliation
from openalex_2025aug..work_author_affiliation as a
join #pub_n_authors as b on a.work_id = b.work_id

drop table if exists #pub_author_affiliation_institution
select a.work_id, a.author_seq, a.affiliation_seq, b.institution_id, [weight] = a.[weight] * cast(1 as float) / count(*) over (partition by a.work_id, a.author_seq, a.affiliation_seq)
into #pub_author_affiliation_institution
from #pub_author_affiliation as a
join openalex_2025aug..work_affiliation_institution as b on a.work_id = b.work_id and a.affiliation_seq = b.affiliation_seq 



-- Perform insitution unification based on component and joint organizations.

drop table if exists #unified_institution_child_organizations
select
	unified_institution_id = university_openalex_institution_id,
	institution_id = university_openalex_institution_id,
	[weight] = cast(1 as float)
into #unified_institution_child_organizations
from #university
union
select
	university_openalex_institution_id,
	affiliated_organization_openalex_institution_id,
	affiliated_organization_weight
from university_affiliated_organization
where relation_type in ('component', 'joint')

drop table if exists #pub_author_affiliation_unified_institution_child_organizations
select a.work_id, a.author_seq, a.affiliation_seq, a.institution_id, b.unified_institution_id, [weight] = a.[weight] * b.[weight]
into #pub_author_affiliation_unified_institution_child_organizations
from #pub_author_affiliation_institution a
join #unified_institution_child_organizations b on a.institution_id = b.institution_id



-- Perform insitution unification based on associated organizations.

drop table if exists #unified_institution_associated_organizations
select
	unified_institution_id = a.university_openalex_institution_id,
	institution_id = affiliated_organization_openalex_institution_id
into #unified_institution_associated_organizations
from university_affiliated_organization as a
left join #unified_institution_child_organizations as b on a.affiliated_organization_openalex_institution_id = b.institution_id
where relation_type = 'associated'
	and b.institution_id is null

drop table if exists #pub_author_affiliation_unified_institution_associated_organizations
select a.work_id, a.author_seq, a.affiliation_seq, a.institution_id, b.unified_institution_id, [weight] = a.[weight] * cast(1 as float) / count(*) over (partition by a.work_id, a.author_seq, a.affiliation_seq, a.institution_id)
into #pub_author_affiliation_unified_institution_associated_organizations
from #pub_author_affiliation_institution a
join #unified_institution_associated_organizations b on a.institution_id = b.institution_id
join 
(
	select distinct work_id, unified_institution_id
	from #pub_author_affiliation_unified_institution_child_organizations
) c on a.work_id = c.work_id and b.unified_institution_id = c.unified_institution_id



-- Combine the institution unification results based on component, joint, and associated organizations.

drop table if exists #pub_author_affiliation_unified_institution
select a.work_id, a.author_seq, a.affiliation_seq, a.institution_id, unified_institution_id = isnull(b.unified_institution_id, a.institution_id), [weight] = isnull(b.[weight], a.[weight])
into #pub_author_affiliation_unified_institution
from #pub_author_affiliation_institution as a
left join
(
	select work_id, author_seq, affiliation_seq, institution_id, unified_institution_id, [weight]
	from #pub_author_affiliation_unified_institution_child_organizations
	union all
	select work_id, author_seq, affiliation_seq, institution_id, unified_institution_id, [weight]
	from #pub_author_affiliation_unified_institution_associated_organizations
) as b on a.work_id = b.work_id and a.author_seq = b.author_seq and a.affiliation_seq = b.affiliation_seq and a.institution_id = b.institution_id

-- Check if the weights add up to the expected value.
if abs((select sum([weight]) from #pub_author_affiliation_unified_institution) - (select sum([weight]) from #pub_author_affiliation_institution)) > 0.001
begin
	raiserror('Unexpected total weight.', 2, 1)
end



-- Select universities that have at least 1500 publications in the most recent period.

drop table if exists #pub_university
select a.work_id, b.university_openalex_institution_id, [weight] = sum(a.[weight])
into #pub_university
from #pub_author_affiliation_unified_institution as a
join #university as b on a.unified_institution_id = b.university_openalex_institution_id
group by a.work_id, b.university_openalex_institution_id

drop table if exists #university_n_pubs
select c.university_id, c.university_ror_id, a.university_openalex_institution_id, n_pubs_full = count(*), n_pubs_frac = sum([weight])
into #university_n_pubs
from #pub_university as a
join pub as b on a.work_id = b.work_id
join #university as c on a.university_openalex_institution_id = c.university_openalex_institution_id
where b.pub_year between @last_period_begin_year and @last_period_end_year
group by c.university_id, c.university_ror_id, a.university_openalex_institution_id
having count(*) >= 1500



-- Create university table. Each row in this table represents a university.

drop table if exists university
create table university
(
	university_id int not null,
	university nvarchar(60) not null,
	university_full_name nvarchar(130) not null,
	university_ror_id char(9) not null,
	university_ror_name nvarchar(130) not null,
	university_openalex_institution_id bigint not null,
	country_code char(2) not null,
	latitude float not null,
	longitude float not null,
	is_mtor_university bit not null
)

insert into university with(tablock)
select
	a.university_id,
	a.university,
	a.university_full_name,
	b.ror_id,
	b.organization,
	c.institution_id,
	a.country_code,
	a.latitude,
	a.longitude,
	a.is_mtor_university
from projectdb_leiden_ranking_open_edition..LROE2025_university_20251024 as a
left join ror_2025aug..organization as b on a.ror_id = b.ror_id
left join #institution_ror_id as c on a.ror_id = c.ror_id
join #university_n_pubs as d on a.ror_id = d.university_ror_id

alter table university add constraint pk_university primary key(university_id)



-- Create pub_university table. This table links publications to universities.

drop table if exists pub_university
create table pub_university
(
	work_id bigint not null,
	university_id int not null,
	[weight] float not null
)

insert into pub_university with(tablock)
select a.work_id, b.university_id, a.[weight]
from #pub_university as a
join university as b on a.university_openalex_institution_id = b.university_openalex_institution_id

alter table pub_university add constraint pk_pub_university primary key(work_id, university_id)



-- Calculate collab and int_collab indicators for each publication.

-- Map Hong Kong and Macao to China.
drop table if exists #country
select country_iso_alpha2_code, cleaned_country_iso_alpha2_code = country_iso_alpha2_code
into #country
from openalex_2025aug..country
where country_iso_alpha2_code not in ('cn', 'hk', 'mo')  -- China, Hong Kong, Macao
union
select country_iso_alpha2_code, 'cn'
from openalex_2025aug..country
where country_iso_alpha2_code in ('cn', 'hk', 'mo')  -- China, Hong Kong, Macao

drop table if exists #pub_country
select a.work_id, c.cleaned_country_iso_alpha2_code
into #pub_country
from #pub_author_affiliation_unified_institution as a
join openalex_2025aug..institution as b on a.institution_id = b.institution_id
join #country as c on b.country_iso_alpha2_code = c.country_iso_alpha2_code
union
select a.work_id, c.cleaned_country_iso_alpha2_code
from pub as a
join openalex_2025aug..work_author_country as b on a.work_id = b.work_id
join #country as c on b.country_iso_alpha2_code = c.country_iso_alpha2_code 

drop table if exists #pub_n_countries
select work_id, n_countries = count(distinct cleaned_country_iso_alpha2_code)
into #pub_n_countries
from #pub_country
group by work_id

drop table if exists #pub_int_collab
select a.work_id, p_int_collab = cast(case when b.n_countries > 1 then 1 else 0 end as float)
into #pub_int_collab
from pub as a
left join #pub_n_countries as b on a.work_id = b.work_id

drop table if exists #pub_n_unified_institutions
select work_id, n_unified_institution = count(distinct unified_institution_id)
into #pub_n_unified_institutions
from #pub_author_affiliation_unified_institution
group by work_id

drop table if exists #pub_collab
select a.work_id, p_collab = cast(case when b.n_unified_institution > 1 or c.n_countries > 1 then 1 else 0 end as float)
into #pub_collab
from pub as a
left join #pub_n_unified_institutions as b on a.work_id = b.work_id
left join #pub_n_countries as c on a.work_id = c.work_id



-- Create pub_collab_indicators table. This table contains the collaboration indicators for each publication.

drop table if exists pub_collab_indicators
create table pub_collab_indicators
(
	work_id bigint not null,
	p_collab float not null,
	p_int_collab float not null,
	p_industry float not null,
	p_short_dist_collab float not null,
	p_long_dist_collab float not null
)

insert into pub_collab_indicators with(tablock)
select
	a.work_id,
	a.p_collab,
	b.p_int_collab,
	p_industry = c.is_industry,
	p_short_dist_collab = cast((case when a.p_collab > 0 and c.gcd <= 100 then 1 else 0 end) as float),
	p_long_dist_collab = cast((case when a.p_collab > 0 and c.gcd >= 5000 then 1 else 0 end) as float)
from #pub_collab as a
join #pub_int_collab as b on a.work_id = b.work_id
join openalex_2025aug_indicators..pub as c on a.work_id = c.work_id

alter table pub_collab_indicators add constraint pk_pub_collab_indicators primary key(work_id)



-- Calculate open access indicators.

drop table if exists #pub_oa
select
	a.work_id,
	p_gold_oa = cast((case when c.oa_status in ('gold', 'diamond') then 1 else 0 end) as float),
	p_hybrid_oa = cast((case when c.oa_status = 'hybrid' then 1 else 0 end) as float),
	p_bronze_oa = cast((case when c.oa_status = 'bronze' then 1 else 0 end) as float),
	p_green_oa = cast((case when c.oa_status = 'green' then 1 else 0 end) as float),
	p_oa_unknown = cast((case when c.oa_status is null then 1 else 0 end) as float)
into #pub_oa
from pub as a
join openalex_2025aug..work as b on a.work_id = b.work_id
left join openalex_2025aug..oa_status as c on b.oa_status_id = c.oa_status_id



-- Create pub_oa_indicators table. This table contains the open access indicators for each publication.

drop table if exists pub_oa_indicators
create table pub_oa_indicators
(
	work_id bigint not null,
	p_oa_unknown float not null,
	p_oa float not null,
	p_gold_oa float not null,
	p_hybrid_oa float not null,
	p_bronze_oa float not null,
	p_green_oa float not null
)

insert into pub_oa_indicators with(tablock)
select distinct
	work_id,
	p_oa_unknown,
	p_oa = cast((case when p_gold_oa + p_hybrid_oa + p_bronze_oa + p_green_oa > 0 then 1 else 0 end) as float),
	p_gold_oa, p_hybrid_oa, p_bronze_oa, p_green_oa
from #pub_oa

alter table pub_oa_indicators add constraint pk_pub_oa_indicators primary key(work_id)



-- Calculate impact, collaboration, and open access indicators for the set of core publications.

use openalex_2025aug_indicators

drop table if exists #pub_period_indicators_core_pubs
create table #pub_period_indicators_core_pubs
(
	work_id bigint not null,
	pub_year int not null,
	period_begin_year int not null,
	cs float,
	p_10_cits float,
	p_20_cits float,
	p_50_cits float,
	p_100_cits float,
	ncs float,
	p_top_1 float,
	p_top_5 float,
	p_top_10 float,
	p_top_50 float,
	p_collab float,
	p_int_collab float,
	p_industry float,
	gcd float,
	p_gold_oa float,
	p_hybrid_oa float,
	p_bronze_oa float,
	p_green_oa float,
	p_oa_unknown float
)

declare @pub_table as pub_table
insert @pub_table(work_id)
select work_id
from leiden_ranking_open_edition_2025..pub
where is_core_pub = 1

declare @pub_indicators_table as pub_indicators_table

set @period_begin_year = @first_period_begin_year
declare @end_year int
declare @end_year_cit_window int
while @period_begin_year <= @last_period_begin_year
begin
	set @end_year = @period_begin_year + @period_n_years - 1
	set @end_year_cit_window = @period_begin_year + @period_n_years

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 2,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 10,
		@top_prop = 0.01

	drop table if exists #pub_indicators1
	select *
	into #pub_indicators1
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 2,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 20,
		@top_prop = 0.05

	drop table if exists #pub_indicators2
	select *
	into #pub_indicators2
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 2,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 50,
		@top_prop = 0.1

	drop table if exists #pub_indicators3
	select *
	into #pub_indicators3
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 2,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 100,
		@top_prop = 0.5

	drop table if exists #pub_indicators4
	select *
	into #pub_indicators4
	from @pub_indicators_table

	drop table if exists #pub_indicators
	select
		a.work_id,
		a.pub_year,
		period_begin_year = @period_begin_year,
		b.cs,
		p_10_cits = b.p_top_n_cits,
		p_20_cits = c.p_top_n_cits,
		p_50_cits = d.p_top_n_cits,
		p_100_cits = e.p_top_n_cits,
		b.ncs,
		p_top_1 = b.p_top_prop,
		p_top_5 = c.p_top_prop,
		p_top_10 = d.p_top_prop,
		p_top_50 = e.p_top_prop,
		b.p_collab,
		b.p_int_collab,
		b.p_industry,
		b.gcd,
		b.p_gold_oa,
		b.p_hybrid_oa,
		b.p_bronze_oa,
		b.p_green_oa,
		b.p_oa_unknown
	into #pub_indicators
	from leiden_ranking_open_edition_2025..pub as a
	join #pub_indicators1 as b on a.work_id = b.work_id
	join #pub_indicators2 as c on a.work_id = c.work_id
	join #pub_indicators3 as d on a.work_id = d.work_id
	join #pub_indicators4 as e on a.work_id = e.work_id

	insert #pub_period_indicators_core_pubs with(tablock)
	select *
	from #pub_indicators

	set @period_begin_year += 1
end



-- Calculate impact, collaboration, and open access indicators for the set of core and non-core publications.

drop table if exists #pub_period_indicators_all_pubs
create table #pub_period_indicators_all_pubs
(
	work_id bigint not null,
	pub_year int not null,
	period_begin_year int not null,
	cs float,
	p_10_cits float,
	p_20_cits float,
	p_50_cits float,
	p_100_cits float,
	ncs float,
	p_top_1 float,
	p_top_5 float,
	p_top_10 float,
	p_top_50 float,
	p_collab float,
	p_int_collab float,
	p_industry float,
	gcd float,
	p_gold_oa float,
	p_hybrid_oa float,
	p_bronze_oa float,
	p_green_oa float,
	p_oa_unknown float
)

delete @pub_table
insert @pub_table(work_id)
select work_id
from leiden_ranking_open_edition_2025..pub

set @period_begin_year = @first_period_begin_year
while @period_begin_year <= @last_period_begin_year
begin
	set @end_year = @period_begin_year + @period_n_years - 1
	set @end_year_cit_window = @period_begin_year + @period_n_years

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 1,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 10,
		@top_prop = 0.01

	drop table if exists #pub_indicators1
	select *
	into #pub_indicators1
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 1,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 20,
		@top_prop = 0.05

	drop table if exists #pub_indicators2
	select *
	into #pub_indicators2
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 1,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 50,
		@top_prop = 0.1

	drop table if exists #pub_indicators3
	select *
	into #pub_indicators3
	from @pub_indicators_table

	delete @pub_indicators_table
	insert @pub_indicators_table
	exec calc_indicators_pubs
		@pub_table = @pub_table,
		@database_no = 1,
		@classification_system_no = 2,
		@pub_window_begin = @period_begin_year,
		@pub_window_end = @end_year,
		@cit_window_end = @end_year_cit_window,
		@top_n_cits = 100,
		@top_prop = 0.5

	drop table if exists #pub_indicators4
	select *
	into #pub_indicators4
	from @pub_indicators_table

	drop table if exists #pub_indicators
	select
		a.work_id,
		a.pub_year,
		period_begin_year = @period_begin_year,
		b.cs,
		p_10_cits = b.p_top_n_cits,
		p_20_cits = c.p_top_n_cits,
		p_50_cits = d.p_top_n_cits,
		p_100_cits = e.p_top_n_cits,
		b.ncs,
		p_top_1 = b.p_top_prop,
		p_top_5 = c.p_top_prop,
		p_top_10 = d.p_top_prop,
		p_top_50 = e.p_top_prop,
		b.p_collab,
		b.p_int_collab,
		b.p_industry,
		b.gcd,
		b.p_gold_oa,
		b.p_hybrid_oa,
		b.p_bronze_oa,
		b.p_green_oa,
		b.p_oa_unknown
	into #pub_indicators
	from leiden_ranking_open_edition_2025..pub as a
	join #pub_indicators1 as b on a.work_id = b.work_id
	join #pub_indicators2 as c on a.work_id = c.work_id
	join #pub_indicators3 as d on a.work_id = d.work_id
	join #pub_indicators4 as e on a.work_id = e.work_id

	insert #pub_period_indicators_all_pubs with(tablock)
	select *
	from #pub_indicators

	set @period_begin_year += 1
end

use leiden_ranking_open_edition_2025



-- Create pub_impact_indicators table. This table contains the impact indicators for each publication-period-core_pubs_only combination.

drop table if exists pub_impact_indicators
create table pub_impact_indicators
(
	work_id bigint not null,
	period_begin_year int not null,
	core_pubs_only bit not null,
	cs float not null,
	p_10_cits float null,
	p_20_cits float null,
	p_50_cits float null,
	p_100_cits float null,
	ncs float null,
	p_top_1 float null,
	p_top_5 float null,
	p_top_10 float null,
	p_top_50 float null
)

insert into pub_impact_indicators with(tablock) (work_id, period_begin_year, core_pubs_only, cs, ncs, p_top_1, p_top_5, p_top_10, p_top_50)
select work_id, period_begin_year, 1, cs, ncs, p_top_1, p_top_5, p_top_10, p_top_50
from #pub_period_indicators_core_pubs

insert into pub_impact_indicators with(tablock) (work_id, period_begin_year, core_pubs_only, cs, p_10_cits, p_20_cits, p_50_cits, p_100_cits)
select work_id, period_begin_year, 0, cs, p_10_cits, p_20_cits, p_50_cits, p_100_cits
from #pub_period_indicators_all_pubs

alter table pub_impact_indicators add constraint pk_pub_impact_indicators primary key(work_id, period_begin_year, core_pubs_only)



-- Calculate impact, collaboration, and open access indicators using full counting and fractional counting for each publication-university-main_field-period-core_pubs_only combination.

drop table if exists #pub_university_main_field_period_indicators
select
	a.work_id,
	a.pub_year,
	b.university_id,
	c.main_field_id,
	d.period_begin_year,
	d.core_pubs_only,
	weight_university = b.[weight],
	weight_main_field = c.[weight],
	d.cs,
	p_10_cits = isnull(d.p_10_cits, 0),
	p_20_cits = isnull(d.p_20_cits, 0),
	p_50_cits = isnull(d.p_50_cits, 0),
	p_100_cits = isnull(d.p_100_cits, 0),
	ncs = isnull(d.ncs, 0),
	p_top_1 = isnull(d.p_top_1, 0),
	p_top_5 = isnull(d.p_top_5, 0),
	p_top_10 = isnull(d.p_top_10, 0),
	p_top_50 = isnull(d.p_top_50, 0),
	e.p_collab,
	e.p_int_collab,
	e.p_industry,
	e.p_short_dist_collab,
	e.p_long_dist_collab,
	f.p_oa_unknown,
	f.p_oa,
	f.p_gold_oa,
	f.p_hybrid_oa,
	f.p_bronze_oa,
	f.p_green_oa
into #pub_university_main_field_period_indicators
from pub as a
join pub_university as b on a.work_id = b.work_id
join pub_main_field as c on a.work_id = c.work_id
join pub_impact_indicators as d on a.work_id = d.work_id
join pub_collab_indicators as e on a.work_id = e.work_id
join pub_oa_indicators as f on a.work_id = f.work_id

create nonclustered index idx_tmp_pub_university_main_field_period_indicators
on #pub_university_main_field_period_indicators (period_begin_year, university_id, main_field_id, core_pubs_only)
include (weight_university, weight_main_field, cs, p_10_cits, p_20_cits, p_50_cits, p_100_cits, ncs, p_top_1, p_top_5, p_top_10, p_top_50)

drop table if exists #university_main_field_period
select
	university_main_field_period_core_pubs_only_no = cast(row_number() over (order by a.university_id, b.main_field_id, c.period_begin_year, d.core_pubs_only) as int),
	a.university_id,
	b.main_field_id,
	c.period_begin_year,
	d.core_pubs_only
into #university_main_field_period
from university as a
cross join main_field as b
cross join [period] as c
cross join
(
	select core_pubs_only = cast(0 as bit)
    union all
    select cast(1 as bit)
) as d

drop table if exists #university_main_field_period2
select
	a.university_main_field_period_core_pubs_only_no,
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	a.core_pubs_only,
	full_p = isnull(b.full_p, 0),
	frac_p = isnull(b.frac_p, 0)
into #university_main_field_period2
from #university_main_field_period as a
left join
(
	select university_id, main_field_id, period_begin_year, core_pubs_only, full_p = sum(weight_main_field), frac_p = sum(weight_university * weight_main_field)
	from #pub_university_main_field_period_indicators
	group by university_id, main_field_id, period_begin_year, core_pubs_only
) as b on a.university_id = b.university_id
	and a.main_field_id = b.main_field_id
	and a.period_begin_year = b.period_begin_year
	and a.core_pubs_only = b.core_pubs_only

create nonclustered index idx_tmp_university_main_field_period2
on #university_main_field_period2 (period_begin_year, university_id, main_field_id, core_pubs_only)

drop table if exists #university_main_field_period_indicators_frac
create table #university_main_field_period_indicators_frac
(
	university_main_field_period_core_pubs_only_no int,
	cs_avg float,
	cs_lb float,
	cs_ub float,
	p_10_cists_avg float,
	p_10_cists_lb float,
	p_10_cists_ub float,
	p_20_cists_avg float,
	p_20_cists_lb float,
	p_20_cists_ub float,
	p_50_cists_avg float,
	p_50_cists_lb float,
	p_50_cists_ub float,
	p_100_cists_avg float,
	p_100_cists_lb float,
	p_100_cists_ub float,
	ncs_avg float,
	ncs_lb float,
	ncs_ub float,
	p_top_1_avg float,
	p_top_1_lb float,
	p_top_1_ub float,
	p_top_5_avg float,
	p_top_5_lb float,
	p_top_5_ub float,
	p_top_10_avg float,
	p_top_10_lb float,
	p_top_10_ub float,
	p_top_50_avg float,
	p_top_50_lb float,
	p_top_50_ub float
)

set @period_begin_year = @first_period_begin_year
while @period_begin_year <= @last_period_begin_year
begin
	drop table if exists #bootstrap_input
	select
		pub_set_no = a.university_main_field_period_core_pubs_only_no,
		[weight] = b.weight_university * b.weight_main_field,
		b.cs,
		b.p_10_cits,
		b.p_20_cits,
		b.p_50_cits,
		b.p_100_cits,
		b.ncs,
		b.p_top_1,
		b.p_top_5,
		b.p_top_10,
		b.p_top_50
	into #bootstrap_input
	from #university_main_field_period2 as a
	join #pub_university_main_field_period_indicators as b on a.university_id = b.university_id
		and a.main_field_id = b.main_field_id
		and a.period_begin_year = b.period_begin_year
		and a.core_pubs_only = b.core_pubs_only
	where a.period_begin_year = @period_begin_year

	insert into #university_main_field_period_indicators_frac
	exec projectdb_leiden_ranking_open_edition..calc_stability_intervals
		@coverage_prob = 0.95,
		@n_bootstrap_samples = 1000

	set @period_begin_year += 1
end

drop table if exists #university_main_field_period_indicators_full
create table #university_main_field_period_indicators_full
(
	university_main_field_period_core_pubs_only_no int,
	cs_avg float,
	cs_lb float,
	cs_ub float,
	p_10_cists_avg float,
	p_10_cists_lb float,
	p_10_cists_ub float,
	p_20_cists_avg float,
	p_20_cists_lb float,
	p_20_cists_ub float,
	p_50_cists_avg float,
	p_50_cists_lb float,
	p_50_cists_ub float,
	p_100_cists_avg float,
	p_100_cists_lb float,
	p_100_cists_ub float,
	ncs_avg float,
	ncs_lb float,
	ncs_ub float,
	p_top_1_avg float,
	p_top_1_lb float,
	p_top_1_ub float,
	p_top_5_avg float,
	p_top_5_lb float,
	p_top_5_ub float,
	p_top_10_avg float,
	p_top_10_lb float,
	p_top_10_ub float,
	p_top_50_avg float,
	p_top_50_lb float,
	p_top_50_ub float,
	p_collab_avg float,
	p_collab_lb float,
	p_collab_ub float,
	p_int_collab_avg float,
	p_int_collab_lb float,
	p_int_collab_ub float,
	p_industry_avg float,
	p_industry_lb float,
	p_industry_ub float,
	p_short_dist_collab_avg float,
	p_short_dist_collab_lb float,
	p_short_dist_collab_ub float,
	p_long_dist_collab_avg float,
	p_long_dist_collab_lb float,
	p_long_dist_collab_ub float,
	p_oa_unknown_avg float,
	p_oa_unknown_lb float,
	p_oa_unknown_ub float,
	p_oa_avg float,
	p_oa_lb float,
	p_oa_ub float,
	p_gold_oa_avg float,
	p_gold_oa_lb float,
	p_gold_oa_ub float,
	p_hybrid_oa_avg float,
	p_hybrid_oa_lb float,
	p_hybrid_oa_ub float,
	p_bronze_oa_avg float,
	p_bronze_oa_lb float,
	p_bronze_oa_ub float,
	p_green_oa_avg float,
	p_green_oa_lb float,
	p_green_oa_ub float
)

set @period_begin_year = @first_period_begin_year
while @period_begin_year <= @last_period_begin_year
begin
	drop table if exists #bootstrap_input
	select
		pub_set_no = a.university_main_field_period_core_pubs_only_no,
		[weight] = b.weight_main_field,
		b.cs,
		b.p_10_cits,
		b.p_20_cits,
		b.p_50_cits,
		b.p_100_cits,
		b.ncs,
		b.p_top_1,
		b.p_top_5,
		b.p_top_10,
		b.p_top_50,
		b.p_collab,
		b.p_int_collab,
		b.p_industry,
		b.p_short_dist_collab,
		b.p_long_dist_collab,
		b.p_oa_unknown,
		b.p_oa,
		b.p_gold_oa,
		b.p_hybrid_oa,
		b.p_bronze_oa,
		b.p_green_oa
	into #bootstrap_input
	from #university_main_field_period2 as a
	join #pub_university_main_field_period_indicators as b on a.university_id = b.university_id
		and a.main_field_id = b.main_field_id
		and a.period_begin_year = b.period_begin_year
		and a.core_pubs_only = b.core_pubs_only
	where a.period_begin_year = @period_begin_year

	insert into #university_main_field_period_indicators_full
	exec projectdb_leiden_ranking_open_edition..calc_stability_intervals
		@coverage_prob = 0.95,
		@n_bootstrap_samples = 1000

	set @period_begin_year += 1
end



-- Create university_impact_indicators table. This table contains the impact indicators for each university-main_field-period-core_pubs_only-fractional_counting combination.

drop table if exists university_impact_indicators
create table university_impact_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
	core_pubs_only bit not null,
	fractional_counting bit not null,
	p float not null,
	tcs float null,
	p_10_cits float null,
	p_20_cits float null,
	p_50_cits float null,
	p_100_cits float null,
	tncs float null,
	p_top_1 float null,
	p_top_5 float null,
	p_top_10 float null,
	p_top_50 float null,
	mcs float null,
	mcs_lb float null,
	mcs_ub float null,
	pp_10_cits float null,
	pp_10_cits_lb float null,
	pp_10_cits_ub float null,
	pp_20_cits float null,
	pp_20_cits_lb float null,
	pp_20_cits_ub float null,
	pp_50_cits float null,
	pp_50_cits_lb float null,
	pp_50_cits_ub float null,
	pp_100_cits float null,
	pp_100_cits_lb float null,
	pp_100_cits_ub float null,
	mncs float null,
	mncs_lb float null,
	mncs_ub float null,
	pp_top_1 float null,
	pp_top_1_lb float null,
	pp_top_1_ub float null,
	pp_top_5 float null,
	pp_top_5_lb float null,
	pp_top_5_ub float null,
	pp_top_10 float null,
	pp_top_10_lb float null,
	pp_top_10_ub float null,
	pp_top_50 float null,
	pp_top_50_lb float null,
	pp_top_50_ub float null,
)

-- Full counting.
insert into university_impact_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	a.core_pubs_only,
	fractional_counting = cast(0 as bit),
	p = a.full_p,
	tcs = a.full_p * b.cs_avg,
	p_10_cits = case when a.core_pubs_only = 0 then a.full_p * b.p_10_cists_avg else null end,
	p_20_cits = case when a.core_pubs_only = 0 then a.full_p * b.p_20_cists_avg else null end,
	p_50_cits = case when a.core_pubs_only = 0 then a.full_p * b.p_50_cists_avg else null end,
	p_100_cits = case when a.core_pubs_only = 0 then a.full_p * b.p_100_cists_avg else null end,
	tncs = case when a.core_pubs_only = 1 then a.full_p * b.ncs_avg else null end,
	p_top_1 = case when a.core_pubs_only = 1 then a.full_p * b.p_top_1_avg else null end,
	p_top_5 = case when a.core_pubs_only = 1 then a.full_p * b.p_top_5_avg else null end,
	p_top_10 = case when a.core_pubs_only = 1 then a.full_p * b.p_top_10_avg else null end,
	p_top_50 = case when a.core_pubs_only = 1 then a.full_p * b.p_top_50_avg else null end,
	mcs = b.cs_avg,
	mcs_lb = b.cs_lb,
	mcs_ub = b.cs_ub,
	pp_10_cits = case when a.core_pubs_only = 0 then b.p_10_cists_avg else null end,
	pp_10_cits_lb = case when a.core_pubs_only = 0 then b.p_10_cists_lb else null end,
	pp_10_cits_ub = case when a.core_pubs_only = 0 then b.p_10_cists_ub else null end,
	pp_20_cits = case when a.core_pubs_only = 0 then b.p_20_cists_avg else null end,
	pp_20_cits_lb = case when a.core_pubs_only = 0 then b.p_20_cists_lb else null end,
	pp_20_cits_ub = case when a.core_pubs_only = 0 then b.p_20_cists_ub else null end,
	pp_50_cits = case when a.core_pubs_only = 0 then b.p_50_cists_avg else null end,
	pp_50_cits_lb = case when a.core_pubs_only = 0 then b.p_50_cists_lb else null end,
	pp_50_cits_ub = case when a.core_pubs_only = 0 then b.p_50_cists_ub else null end,
	pp_100_cits = case when a.core_pubs_only = 0 then b.p_100_cists_avg else null end,
	pp_100_cits_lb = case when a.core_pubs_only = 0 then b.p_100_cists_lb else null end,
	pp_100_cits_ub = case when a.core_pubs_only = 0 then b.p_100_cists_ub else null end,
	mncs = case when a.core_pubs_only = 1 then b.ncs_avg else null end,
	mncs_lb = case when a.core_pubs_only = 1 then b.ncs_lb else null end,
	mncs_ub = case when a.core_pubs_only = 1 then b.ncs_ub else null end,
	pp_top_1 = case when a.core_pubs_only = 1 then b.p_top_1_avg else null end,
	pp_top_1_lb = case when a.core_pubs_only = 1 then b.p_top_1_lb else null end,
	pp_top_1_ub = case when a.core_pubs_only = 1 then b.p_top_1_ub else null end,
	pp_top_5 = case when a.core_pubs_only = 1 then b.p_top_5_avg else null end,
	pp_top_5_lb = case when a.core_pubs_only = 1 then b.p_top_5_lb else null end,
	pp_top_5_ub = case when a.core_pubs_only = 1 then b.p_top_5_ub else null end,
	pp_top_10 = case when a.core_pubs_only = 1 then b.p_top_10_avg else null end,
	pp_top_10_lb = case when a.core_pubs_only = 1 then b.p_top_10_lb else null end,
	pp_top_10_ub = case when a.core_pubs_only = 1 then b.p_top_10_ub else null end,
	pp_top_50 = case when a.core_pubs_only = 1 then b.p_top_50_avg else null end,
	pp_top_50_lb = case when a.core_pubs_only = 1 then b.p_top_50_lb else null end,
	pp_top_50_ub = case when a.core_pubs_only = 1 then b.p_top_50_ub else null end
from #university_main_field_period2 as a
join #university_main_field_period_indicators_full as b on a.university_main_field_period_core_pubs_only_no = b.university_main_field_period_core_pubs_only_no

-- Fractional counting.
insert into university_impact_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	a.core_pubs_only,
	fractional_counting = cast(1 as bit),
	p = a.frac_p,
	tcs = a.frac_p * b.cs_avg,
	p_10_cits = case when a.core_pubs_only = 0 then a.frac_p * b.p_10_cists_avg else null end,
	p_20_cits = case when a.core_pubs_only = 0 then a.frac_p * b.p_20_cists_avg else null end,
	p_50_cits = case when a.core_pubs_only = 0 then a.frac_p * b.p_50_cists_avg else null end,
	p_100_cits = case when a.core_pubs_only = 0 then a.frac_p * b.p_100_cists_avg else null end,
	tncs = case when a.core_pubs_only = 1 then a.frac_p * b.ncs_avg else null end,
	p_top_1 = case when a.core_pubs_only = 1 then a.frac_p * b.p_top_1_avg else null end,
	p_top_5 = case when a.core_pubs_only = 1 then a.frac_p * b.p_top_5_avg else null end,
	p_top_10 = case when a.core_pubs_only = 1 then a.frac_p * b.p_top_10_avg else null end,
	p_top_50 = case when a.core_pubs_only = 1 then a.frac_p * b.p_top_50_avg else null end,
	mcs = b.cs_avg,
	mcs_lb = b.cs_lb,
	mcs_ub = b.cs_ub,
	pp_10_cits = case when a.core_pubs_only = 0 then b.p_10_cists_avg else null end,
	pp_10_cits_lb = case when a.core_pubs_only = 0 then b.p_10_cists_lb else null end,
	pp_10_cits_ub = case when a.core_pubs_only = 0 then b.p_10_cists_ub else null end,
	pp_20_cits = case when a.core_pubs_only = 0 then b.p_20_cists_avg else null end,
	pp_20_cits_lb = case when a.core_pubs_only = 0 then b.p_20_cists_lb else null end,
	pp_20_cits_ub = case when a.core_pubs_only = 0 then b.p_20_cists_ub else null end,
	pp_50_cits = case when a.core_pubs_only = 0 then b.p_50_cists_avg else null end,
	pp_50_cits_lb = case when a.core_pubs_only = 0 then b.p_50_cists_lb else null end,
	pp_50_cits_ub = case when a.core_pubs_only = 0 then b.p_50_cists_ub else null end,
	pp_100_cits = case when a.core_pubs_only = 0 then b.p_100_cists_avg else null end,
	pp_100_cits_lb = case when a.core_pubs_only = 0 then b.p_100_cists_lb else null end,
	pp_100_cits_ub = case when a.core_pubs_only = 0 then b.p_100_cists_ub else null end,
	mncs = case when a.core_pubs_only = 1 then b.ncs_avg else null end,
	mncs_lb = case when a.core_pubs_only = 1 then b.ncs_lb else null end,
	mncs_ub = case when a.core_pubs_only = 1 then b.ncs_ub else null end,
	pp_top_1 = case when a.core_pubs_only = 1 then b.p_top_1_avg else null end,
	pp_top_1_lb = case when a.core_pubs_only = 1 then b.p_top_1_lb else null end,
	pp_top_1_ub = case when a.core_pubs_only = 1 then b.p_top_1_ub else null end,
	pp_top_5 = case when a.core_pubs_only = 1 then b.p_top_5_avg else null end,
	pp_top_5_lb = case when a.core_pubs_only = 1 then b.p_top_5_lb else null end,
	pp_top_5_ub = case when a.core_pubs_only = 1 then b.p_top_5_ub else null end,
	pp_top_10 = case when a.core_pubs_only = 1 then b.p_top_10_avg else null end,
	pp_top_10_lb = case when a.core_pubs_only = 1 then b.p_top_10_lb else null end,
	pp_top_10_ub = case when a.core_pubs_only = 1 then b.p_top_10_ub else null end,
	pp_top_50 = case when a.core_pubs_only = 1 then b.p_top_50_avg else null end,
	pp_top_50_lb = case when a.core_pubs_only = 1 then b.p_top_50_lb else null end,
	pp_top_50_ub = case when a.core_pubs_only = 1 then b.p_top_50_ub else null end
from #university_main_field_period2 as a
join #university_main_field_period_indicators_frac as b on a.university_main_field_period_core_pubs_only_no = b.university_main_field_period_core_pubs_only_no

alter table university_impact_indicators add constraint pk_university_impact_indicators primary key(university_id, main_field_id, period_begin_year, core_pubs_only, fractional_counting)



-- Create university_collab_indicators table. This table contains the collaboration indicators for each university-main_field-period-core_pubs_only combination.

drop table if exists university_collab_indicators
create table university_collab_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
	core_pubs_only bit not null,
	p float not null,
	p_collab float null,
	p_int_collab float null,
	p_industry_collab float null,
	p_short_dist_collab float null,
	p_long_dist_collab float null,
	pp_collab float null,
	pp_collab_lb float null,
	pp_collab_ub float null,
	pp_int_collab float null,
	pp_int_collab_lb float null,
	pp_int_collab_ub float null,
	pp_industry_collab float null,
	pp_industry_collab_lb float null,
	pp_industry_collab_ub float null,
	pp_short_dist_collab float null,
	pp_short_dist_collab_lb float null,
	pp_short_dist_collab_ub float null,
	pp_long_dist_collab float null,
	pp_long_dist_collab_lb float null,
	pp_long_dist_collab_ub float null,
)

insert into university_collab_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	a.core_pubs_only,
	p = a.full_p,
	p_collab = a.full_p * b.p_collab_avg,
	p_int_collab = a.full_p * b.p_int_collab_avg,
	p_industry_collab = a.full_p * b.p_industry_avg,
	p_short_dist_collab = a.full_p * b.p_short_dist_collab_avg,
	p_long_dist_collab = a.full_p * b.p_long_dist_collab_avg,
	pp_collab = b.p_collab_avg,
	pp_collab_lb = b.p_collab_lb,
	pp_collab_ub = b.p_collab_ub,
	pp_int_collab = b.p_int_collab_avg,
	pp_int_collab_lb = b.p_int_collab_lb,
	pp_int_collab_ub = b.p_int_collab_ub,
	pp_industry_collab = b.p_industry_avg,
	pp_industry_collab_lb = b.p_industry_lb,
	pp_industry_collab_ub = b.p_industry_ub,
	pp_short_dist_collab = b.p_short_dist_collab_avg,
	pp_short_dist_collab_lb = b.p_short_dist_collab_lb,
	pp_short_dist_collab_ub = b.p_short_dist_collab_ub,
	pp_long_dist_collab = b.p_long_dist_collab_avg,
	pp_long_dist_collab_lb = b.p_long_dist_collab_lb,
	pp_long_dist_collab_ub = b.p_long_dist_collab_ub
from #university_main_field_period2 as a
join #university_main_field_period_indicators_full as b on a.university_main_field_period_core_pubs_only_no = b.university_main_field_period_core_pubs_only_no

alter table university_collab_indicators add constraint pk_university_collab_indicators primary key(university_id, main_field_id, period_begin_year, core_pubs_only)



-- Create university_oa_indicators table. This table contains the open access indicators for each university-main_field-period-core_pubs_only combination.

drop table if exists university_oa_indicators
create table university_oa_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
	core_pubs_only bit not null,
	p float not null,
	p_oa_unknown float null,
	p_oa float null,
	p_gold_oa float null,
	p_hybrid_oa float null,
	p_bronze_oa float null,
	p_green_oa float null,
	pp_oa_unknown float null,
	pp_oa_unknown_lb float null,
	pp_oa_unknown_ub float null,
	pp_oa float null,
	pp_oa_lb float null,
	pp_oa_ub float null,
	pp_gold_oa float null,
	pp_gold_oa_lb float null,
	pp_gold_oa_ub float null,
	pp_hybrid_oa float null,
	pp_hybrid_oa_lb float null,
	pp_hybrid_oa_ub float null,
	pp_bronze_oa float null,
	pp_bronze_oa_lb float null,
	pp_bronze_oa_ub float null,
	pp_green_oa float null,
	pp_green_oa_lb float null,
	pp_green_oa_ub float null
)

insert into university_oa_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	a.core_pubs_only,
	p = a.full_p,
	p_oa_unknown = a.full_p * b.p_oa_unknown_avg,
	p_oa = a.full_p * b.p_oa_avg,
	p_gold_oa = a.full_p * b.p_gold_oa_avg,
	p_hybrid_oa = a.full_p * b.p_hybrid_oa_avg,
	p_bronze_oa = a.full_p * b.p_bronze_oa_avg,
	p_green_oa = a.full_p * b.p_green_oa_avg,
	pp_oa_unknown = b.p_oa_unknown_avg,
	pp_oa_unknown_lb = b.p_oa_unknown_lb,
	pp_oa_unknown_ub = b.p_oa_unknown_ub,
	pp_oa = b.p_oa_avg,
	pp_oa_lb = b.p_oa_lb,
	pp_oa_ub = b.p_oa_ub,
	pp_gold_oa = b.p_gold_oa_avg,
	pp_gold_oa_lb = b.p_gold_oa_lb,
	pp_gold_oa_ub = b.p_gold_oa_ub,
	pp_hybrid_oa = b.p_hybrid_oa_avg,
	pp_hybrid_oa_lb = b.p_hybrid_oa_lb,
	pp_hybrid_oa_ub = b.p_hybrid_oa_ub,
	pp_bronze_oa = b.p_bronze_oa_avg,
	pp_bronze_oa_lb = b.p_bronze_oa_lb,
	pp_bronze_oa_ub = b.p_bronze_oa_ub,
	pp_green_oa = b.p_green_oa_avg,
	pp_green_oa_lb = b.p_green_oa_lb,
	pp_green_oa_ub = b.p_green_oa_ub
from #university_main_field_period2 as a
join #university_main_field_period_indicators_full as b on a.university_main_field_period_core_pubs_only_no = b.university_main_field_period_core_pubs_only_no

alter table university_oa_indicators add constraint pk_university_oa_indicators primary key(university_id, main_field_id, period_begin_year, core_pubs_only)
