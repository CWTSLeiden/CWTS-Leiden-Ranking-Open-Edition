use leiden_ranking_open_edition_2024
go

declare @period_n_years int = 4
declare @first_period_begin_year int = 2006
declare @last_period_begin_year int = 2019
declare @last_period_end_year int = @last_period_begin_year + @period_n_years - 1



-- Create university table. Each row in this table represents a university.

drop table if exists university
create table university
(
	university_id int not null,
	university nvarchar(60) not null,
	university_full_name nvarchar(70) not null,
	ror_id char(9) not null,
	ror_name nvarchar(70) not null,
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
	ror_name = b.organization,
	a.country_code,
	a.latitude,
	a.longitude,
	a.is_mtor_university
from projectdb_leiden_ranking_open_edition..LROE2024_university_20240923 as a
join ror_2024aug..organization as b on a.ror_id = b.ror_id

alter table university add constraint pk_university primary key(university_id)



-- Create university_affiliated_organization table. Each row in this table represents the relation between a university and an affiliated organization.

drop table if exists university_affiliated_organization
create table university_affiliated_organization
(
	university_ror_id char(9) not null,
	relation_type varchar(10) not null,
	affiliated_organization_ror_id char(9) not null,
	affiliated_organization_weight float null
)

insert into university_affiliated_organization with(tablock)
select distinct
	a.ror_id,
	a.relation_type,
	a.affiliated_organization_ror_id,
	a.affiliated_organization_weight
from projectdb_leiden_ranking_open_edition..LROE2024_university_affiliated_organization_20240923 as a
join ror_2024aug..organization as b on a.affiliated_organization_ror_id = b.ror_id
where a.ror_id <> a.affiliated_organization_ror_id

alter table university_affiliated_organization add constraint pk_university_affiliated_organization primary key(university_ror_id, affiliated_organization_ror_id)



-- Create related_organization table. Each row in this table represents an affiliated (component, joint, or associated) organization.

drop table if exists affiliated_organization
create table affiliated_organization
(
	ror_id char(9) not null,
	ror_name varchar(150) not null
)

insert into affiliated_organization with(tablock)
select a.ror_id, ror_name = b.organization
from
(
	select distinct ror_id = affiliated_organization_ror_id
	from university_affiliated_organization
) as a
join ror_2024aug..organization as b on a.ror_id = b.ror_id

alter table affiliated_organization add constraint pk_affiliated_organization primary key(ror_id)



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
from openalex_2024aug_classification..main_field

alter table main_field add constraint pk_main_field primary key(main_field_id)



-- Create publication table. Each row in this table represents a publication. Only core publications that have not been retrected are taken into account.

drop table if exists pub
create table pub
(
	work_id bigint not null,
	pub_year smallint not null
)

insert into pub with(tablock)
select a.work_id, a.pub_year
from openalex_2024aug..work as a
join openalex_2024aug_core..work as b on a.work_id = b.work_id
where a.pub_year between @first_period_begin_year and @last_period_end_year
	and b.is_core_work = 1  -- Core publication.
	and a.is_retracted = 0  -- Not retracted.

alter table pub add constraint pk_pub primary key(work_id)



-- Link publications to institutions and calculate the weight of linked institutions based on the number of affiliated authors.

drop table if exists #pub_author
select a.work_id, a.author_seq
into #pub_author
from openalex_2024aug..work_author as a
join pub as b on a.work_id = b.work_id

drop table if exists #pub_n_authors
select work_id, n_authors = count(*)
into #pub_n_authors
from #pub_author
group by work_id

drop table if exists #pub_author_n_affiliations
select a.work_id, a.author_seq, n_affiliations = count(*)
into #pub_author_n_affiliations
from openalex_2024aug..work_author_affiliation as a
join pub as b on a.work_id = b.work_id
group by a.work_id, a.author_seq

drop table if exists #pub_author_affiliation
select a.work_id, a.author_seq, a.affiliation_seq, [weight] = cast(1 as float) / b.n_affiliations
into #pub_author_affiliation
from openalex_2024aug..work_author_affiliation as a
join #pub_author_n_affiliations as b on a.work_id = b.work_id and a.author_seq = b.author_seq

drop table if exists #pub_affiliation_n_authors
select work_id, affiliation_seq, n_authors = sum([weight])
into #pub_affiliation_n_authors
from #pub_author_affiliation
group by work_id, affiliation_seq

-- Check if the weights add up to the expected value.
if abs((select sum(n_authors) from #pub_affiliation_n_authors) - (select count(*) from #pub_author_n_affiliations)) > 0.001
begin
	raiserror('Unexpected total weight.', 2, 1)
end

drop table if exists #pub_affiliation
select a.work_id, a.affiliation_seq, [weight] = cast(b.n_authors as float) / c.n_authors
into #pub_affiliation
from openalex_2024aug..work_affiliation as a
join #pub_affiliation_n_authors as b on a.work_id = b.work_id and a.affiliation_seq = b.affiliation_seq
join #pub_n_authors as c on a.work_id = c.work_id

drop table if exists #pub_affiliation_n_institutions
select a.work_id, a.affiliation_seq, n_institutions = count(*)
into #pub_affiliation_n_institutions
from openalex_2024aug..work_affiliation_institution as a
join pub as b on a.work_id = b.work_id
group by a.work_id, a.affiliation_seq

drop table if exists #pub_affiliation_institution
select a.work_id, a.affiliation_seq, a.institution_seq, a.institution_id, [weight] = cast(1 as float) / b.n_institutions
into #pub_affiliation_institution
from openalex_2024aug..work_affiliation_institution as a
join #pub_affiliation_n_institutions as b on a.work_id = b.work_id and a.affiliation_seq = b.affiliation_seq

drop table if exists #pub_affiliation_institution2
select work_id, affiliation_seq, institution_seq, affiliation_institution_seq = row_number() over (partition by work_id order by affiliation_seq, institution_seq), institution_id, [weight]
into #pub_affiliation_institution2
from
(
	select work_id, affiliation_seq, institution_seq, institution_id, [weight]
	from #pub_affiliation_institution
	union all
	select a.work_id, a.affiliation_seq, b.institution_seq, b.institution_id, [weight] = cast(1 as float)
	from #pub_affiliation as a
	left join #pub_affiliation_institution as b on a.work_id = b.work_id and a.affiliation_seq = b.affiliation_seq
	where b.work_id is null
) as a

drop table if exists #pub_institution
select a.work_id, institution_seq = row_number() over (partition by a.work_id order by min(affiliation_institution_seq)), a.institution_id, [weight] = sum(a.[weight] * b.[weight])
into #pub_institution
from #pub_affiliation_institution2 as a
join #pub_affiliation as b on a.work_id = b.work_id and a.affiliation_seq = b.affiliation_seq
group by a.work_id, a.institution_id

-- Check if the weights add up to the expected value.
if abs((select sum([weight]) from #pub_institution) - (select sum([weight]) from #pub_affiliation)) > 0.001
begin
	raiserror('Unexpected total weight.', 2, 1)
end

drop table if exists #pub_institution2
select a.work_id, a.institution_seq, a.institution_id, institution_ror_id = b.ror_id, a.[weight]
into #pub_institution2
from #pub_institution as a
join openalex_2024aug..institution as b on a.institution_id = b.institution_id
where b.ror_id is not null



-- Perform insitution unification based on component and joint organizations.

drop table if exists #unified_institution_child_organizations
select
	unified_institution_ror_id = ror_id,
	institution_ror_id = ror_id,
	[weight] = cast(1 as float)
into #unified_institution_child_organizations
from university
union
select
	university_ror_id,
	affiliated_organization_ror_id,
	affiliated_organization_weight
from university_affiliated_organization
where relation_type in ('component', 'joint')

drop table if exists #pub_unified_institution_child_organizations
select a.work_id, a.institution_seq, b.unified_institution_ror_id, [weight] = a.[weight] * b.[weight]
into #pub_unified_institution_child_organizations
from #pub_institution2 as a
join #unified_institution_child_organizations as b on a.institution_ror_id = b.institution_ror_id



-- Perform insitution unification based on associated organizations.

drop table if exists #unified_institution_associated_organizations
select distinct
	unified_institution_ror_id = university_ror_id,
	institution_ror_id = affiliated_organization_ror_id
into #unified_institution_associated_organizations
from university_affiliated_organization
where relation_type = 'associated'

drop table if exists #pub_unified_institution_associated_organizations
select a.work_id, a.institution_seq, b.unified_institution_ror_id, [weight] = a.[weight]
into #pub_unified_institution_associated_organizations
from #pub_institution2 as a
join #unified_institution_associated_organizations as b on a.institution_ror_id = b.institution_ror_id
join
(
	select distinct work_id, unified_institution_ror_id
	from #pub_unified_institution_child_organizations
) as c on a.work_id = c.work_id and b.unified_institution_ror_id = c.unified_institution_ror_id
left join
(
	select distinct work_id, institution_seq
	from #pub_unified_institution_child_organizations
) as d on a.work_id = d.work_id and a.institution_seq = d.institution_seq
where d.work_id is null

drop table if exists #pub_unified_institution_associated_organizations2
select a.work_id, a.institution_seq, a.unified_institution_ror_id, [weight] = (cast(1 as float) / b.n_organizations) * a.[weight]
into #pub_unified_institution_associated_organizations2
from #pub_unified_institution_associated_organizations as a
join
(
	select work_id, institution_seq, n_organizations = count(*)
	from #pub_unified_institution_associated_organizations
	group by work_id, institution_seq
) as b on a.work_id = b.work_id and a.institution_seq = b.institution_seq



-- Combine the institution unification results based on component, joint, and associated organizations.

drop table if exists #pub_unified_institution
select
	a.work_id,
	a.institution_seq,
	a.institution_id,
	a.institution_ror_id,
	institution_unified_institution_seq = row_number() over (partition by a.work_id, a.institution_seq order by b.unified_institution_ror_id),
	unified_institution_ror_id = isnull(b.unified_institution_ror_id, a.institution_ror_id),
	[weight] = isnull(b.[weight], a.[weight])
into #pub_unified_institution
from #pub_institution2 as a
left join
(
	select work_id, institution_seq, unified_institution_ror_id, [weight]
	from #pub_unified_institution_child_organizations
	union all
	select work_id, institution_seq, unified_institution_ror_id, [weight]
	from #pub_unified_institution_associated_organizations2
) as b on a.work_id = b.work_id and a.institution_seq = b.institution_seq

-- Check if the weights add up to the expected value.
if abs((select sum([weight]) from #pub_unified_institution) - (select sum([weight]) from #pub_institution2)) > 0.001
begin
	raiserror('Unexpected total weight.', 2, 1)
end



-- Create pub_university table. This table links publications to universities.

drop table if exists pub_university
create table pub_university
(
	work_id bigint not null,
	university_id int not null,
	[weight] float not null
)

insert into pub_university with(tablock)
select a.work_id, b.university_id, [weight] = sum(a.[weight])
from #pub_unified_institution as a
join university as b on a.unified_institution_ror_id = b.ror_id
where university_id is not null
group by a.work_id, b.university_id

alter table pub_university add constraint pk_pub_university primary key(work_id, university_id)



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
from pub
union
select a.work_id, main_field_id = d.main_field_id, d.[weight]
from pub as a
join openalex_2024aug_indicators..pub as b on a.work_id = b.work_id
join openalex_2024aug_indicators..pub_classification_system_research_area as c on b.work_id = c.work_id
join openalex_2024aug_classification..micro_cluster_main_field as d on c.research_area_no = d.micro_cluster_id
where c.classification_system_no = 2

alter table pub_main_field add constraint pk_pub_main_field primary key(work_id, main_field_id)



-- Calculate impact, collaboration, and open access indicators for each publication-period combination.

use openalex_2024aug_indicators

drop table if exists #pub_period_indicators
create table #pub_period_indicators
(
	work_id bigint not null,
	pub_year int not null,
	period_begin_year int not null,
	cs float,
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
from leiden_ranking_open_edition_2024..pub

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
	from leiden_ranking_open_edition_2024..pub as a
	join #pub_indicators1 as b on a.work_id = b.work_id
	join #pub_indicators2 as c on a.work_id = c.work_id
	join #pub_indicators3 as d on a.work_id = d.work_id
	join #pub_indicators4 as e on a.work_id = e.work_id

	insert #pub_period_indicators with(tablock)
	select *
	from #pub_indicators

	set @period_begin_year += 1
end

use leiden_ranking_open_edition_2024



-- Calculate collab and int_collab indicators for each publication.

-- Map Hong Kong and Macao to China.
drop table if exists #country
select country_iso_alpha2_code, cleaned_country_iso_alpha2_code = country_iso_alpha2_code
into #country
from openalex_2024aug..country
where country_iso_alpha2_code not in ('cn', 'hk', 'mo')  -- China, Hong Kong, Macao
union
select country_iso_alpha2_code, 'cn'
from openalex_2024aug..country
where country_iso_alpha2_code in ('cn', 'hk', 'mo')  -- China, Hong Kong, Macao

drop table if exists #pub_country
select a.work_id, c.cleaned_country_iso_alpha2_code
into #pub_country
from #pub_institution2 as a
join openalex_2024aug..institution as b on a.institution_id = b.institution_id
join #country as c on b.country_iso_alpha2_code = c.country_iso_alpha2_code
union
select a.work_id, c.cleaned_country_iso_alpha2_code
from pub as a
join openalex_2024aug..work_author_country as b on a.work_id = b.work_id
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
select work_id, n_unified_institution = count(distinct unified_institution_ror_id)
into #pub_n_unified_institutions
from #pub_unified_institution
group by work_id

drop table if exists #pub_collab
select a.work_id, p_collab = cast(case when b.n_unified_institution > 1 or c.n_countries > 1 then 1 else 0 end as float)
into #pub_collab
from pub as a
left join #pub_n_unified_institutions as b on a.work_id = b.work_id
left join #pub_n_countries as c on a.work_id = c.work_id



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
join openalex_2024aug..work as b on a.work_id = b.work_id
left join openalex_2024aug..oa_status as c on b.oa_status_id = c.oa_status_id



-- Collect calculated impact, collaboration, and open access indicators for each publication-period combination.

drop table if exists #pub_period_indicators2
select
	a.work_id,
	a.pub_year,
	a.period_begin_year,
	a.cs,
	a.ncs,
	a.p_top_1,
	a.p_top_5,
	a.p_top_10,
	a.p_top_50,
	b.p_collab,
	c.p_int_collab,
	a.p_industry,
	p_short_dist_collab = cast((case when b.p_collab > 0 and a.gcd <= 100 then 1 else 0 end) as float),
	p_long_dist_collab = cast((case when b.p_collab > 0 and a.gcd >= 5000 then 1 else 0 end) as float),
	d.p_oa_unknown,
	p_oa = cast((case when d.p_gold_oa + d.p_hybrid_oa + d.p_bronze_oa + d.p_green_oa > 0 then 1 else 0 end) as float),
	d.p_gold_oa,
	d.p_hybrid_oa,
	d.p_bronze_oa,
	d.p_green_oa
into #pub_period_indicators2
from #pub_period_indicators as a
join #pub_collab as b on a.work_id = b.work_id
join #pub_int_collab as c on a.work_id = c.work_id
join #pub_oa as d on a.work_id = d.work_id



-- Create pub_period_impact_indicators table. This table contains the impact indicators for each publication-period combination.

drop table if exists pub_period_impact_indicators
create table pub_period_impact_indicators
(
	work_id bigint not null,
	period_begin_year int not null,
	cs float not null,
	ncs float not null,
	p_top_1 float not null,
	p_top_5 float not null,
	p_top_10 float not null,
	p_top_50 float not null,
)

insert into pub_period_impact_indicators with(tablock)
select work_id, period_begin_year, cs, ncs, p_top_1, p_top_5, p_top_10, p_top_50
from #pub_period_indicators2

alter table pub_period_impact_indicators add constraint pk_pub_period_impact_indicators primary key(work_id, period_begin_year)



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
select distinct work_id, p_collab, p_int_collab, p_industry, p_short_dist_collab, p_long_dist_collab
from #pub_period_indicators2

alter table pub_collab_indicators add constraint pk_pub_collab_indicators primary key(work_id)



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
select distinct work_id, p_oa_unknown, p_oa, p_gold_oa, p_hybrid_oa, p_bronze_oa, p_green_oa
from #pub_period_indicators2

alter table pub_oa_indicators add constraint pk_pub_oa_indicators primary key(work_id)



-- Calculate impact, collaboration, and open access indicators using full counting and fractional counting for each publication-university-main_field-period combination.

drop table if exists #pub_university_main_field_period_indicators
select
	a.work_id,
	c.pub_year,
	a.university_id,
	b.main_field_id,
	c.period_begin_year,
	weight_university = a.[weight],
	weight_main_field = b.[weight],
	c.cs,
	c.ncs,
	c.p_top_1,
	c.p_top_5,
	c.p_top_10,
	c.p_top_50,
	c.p_collab,
	c.p_int_collab,
	c.p_industry,
	c.p_short_dist_collab,
	c.p_long_dist_collab,
	c.p_oa_unknown,
	c.p_oa,
	c.p_gold_oa,
	c.p_hybrid_oa,
	c.p_bronze_oa,
	c.p_green_oa
into #pub_university_main_field_period_indicators
from pub_university as a
join pub_main_field as b on a.work_id = b.work_id
join #pub_period_indicators2 as c on a.work_id = c.work_id

drop table if exists #university_main_field_period
select
	university_main_field_begin_year_no = cast(row_number() over (order by a.university_id, b.main_field_id, c.period_begin_year) as int),
	a.university_id,
	b.main_field_id,
	c.period_begin_year
into #university_main_field_period
from university as a
cross join main_field as b
cross join [period] as c

drop table if exists #university_main_field_period2
select
	a.university_main_field_begin_year_no,
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	full_p = isnull(b.full_p, 0),
	frac_p = isnull(b.frac_p, 0)
into #university_main_field_period2
from #university_main_field_period as a
left join
(
	select university_id, main_field_id, period_begin_year, full_p = sum(weight_main_field), frac_p = sum(weight_university * weight_main_field)
	from #pub_university_main_field_period_indicators
	group by university_id, main_field_id, period_begin_year
) as b on a.university_id = b.university_id
	and a.main_field_id = b.main_field_id
	and a.period_begin_year = b.period_begin_year

drop table if exists #university_main_field_period_indicators_frac
create table #university_main_field_period_indicators_frac
(
	university_main_field_begin_year_no int,
	cs_avg float,
	cs_lb float,
	cs_ub float,
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
go

declare @main_field_id int = 0
while @main_field_id <= 5
begin
	drop table if exists #bootstrap_input
	select
		pub_set_no = a.university_main_field_begin_year_no,
		[weight] = b.weight_university * b.weight_main_field,
		b.cs,
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
	where a.main_field_id = @main_field_id

	insert into #university_main_field_period_indicators_frac
	exec projectdb_leiden_ranking_open_edition..calc_stability_intervals
		@coverage_prob = 0.95,
		@n_bootstrap_samples = 1000

	set @main_field_id += 1
end

drop table if exists #university_main_field_period_indicators_full
create table #university_main_field_period_indicators_full
(
	university_main_field_begin_year_no int,
	cs_avg float,
	cs_lb float,
	cs_ub float,
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
go

declare @main_field_id int = 0
while @main_field_id <= 5
begin
	drop table if exists #bootstrap_input
	select
		pub_set_no = a.university_main_field_begin_year_no,
		[weight] = b.weight_main_field,
		b.cs,
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
	where a.main_field_id = @main_field_id

	insert into #university_main_field_period_indicators_full
	exec projectdb_leiden_ranking_open_edition..calc_stability_intervals
		@coverage_prob = 0.95,
		@n_bootstrap_samples = 1000

	set @main_field_id += 1
end



-- Create university_main_field_period_impact_indicators table. This table contains the impact indicators for each university-main_field-period-counting_method combination.

drop table if exists university_main_field_period_impact_indicators
create table university_main_field_period_impact_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
	fractional_counting bit not null,
	p float not null,
	tcs float null,
	tncs float null,
	p_top_1 float null,
	p_top_5 float null,
	p_top_10 float null,
	p_top_50 float null,
	mcs float null,
	mcs_lb float null,
	mcs_ub float null,
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
insert into university_main_field_period_impact_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	fractional_counting = cast(0 as bit),
	p = a.full_p,
	tcs = a.full_p * b.cs_avg,
	tncs = a.full_p * b.ncs_avg,
	p_top_1 = a.full_p * b.p_top_1_avg,
	p_top_5 = a.full_p * b.p_top_5_avg,
	p_top_10 = a.full_p * b.p_top_10_avg,
	p_top_50 = a.full_p * b.p_top_50_avg,
	mcs = b.cs_avg,
	mcs_lb = b.cs_lb,
	mcs_ub = b.cs_ub,
	mncs = b.ncs_avg,
	mncs_lb = b.ncs_lb,
	mncs_ub = b.ncs_ub,
	pp_top_1 = b.p_top_1_avg,
	pp_top_1_lb = b.p_top_1_lb,
	pp_top_1_ub = b.p_top_1_ub,
	pp_top_5 = b.p_top_5_avg,
	pp_top_5_lb = b.p_top_5_lb,
	pp_top_5_ub = b.p_top_5_ub,
	pp_top_10 = b.p_top_10_avg,
	pp_top_10_lb = b.p_top_10_lb,
	pp_top_10_ub = b.p_top_10_ub,
	pp_top_50 = b.p_top_50_avg,
	pp_top_50_lb = b.p_top_50_lb,
	pp_top_50_ub = b.p_top_50_ub
from #university_main_field_period2 as a
left join #university_main_field_period_indicators_full as b on a.university_main_field_begin_year_no = b.university_main_field_begin_year_no

-- Fractional counting.
insert into university_main_field_period_impact_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
	fractional_counting = cast(1 as bit),
	p = a.frac_p,
	tcs = a.frac_p * b.cs_avg,
	tncs = a.frac_p * b.ncs_avg,
	p_top_1 = a.frac_p * b.p_top_1_avg,
	p_top_5 = a.frac_p * b.p_top_5_avg,
	p_top_10 = a.frac_p * b.p_top_10_avg,
	p_top_50 = a.frac_p * b.p_top_50_avg,
	mcs = b.cs_avg,
	mcs_lb = b.cs_lb,
	mcs_ub = b.cs_ub,
	mncs = b.ncs_avg,
	mncs_lb = b.ncs_lb,
	mncs_ub = b.ncs_ub,
	pp_top_1 = b.p_top_1_avg,
	pp_top_1_lb = b.p_top_1_lb,
	pp_top_1_ub = b.p_top_1_ub,
	pp_top_5 = b.p_top_5_avg,
	pp_top_5_lb = b.p_top_5_lb,
	pp_top_5_ub = b.p_top_5_ub,
	pp_top_10 = b.p_top_10_avg,
	pp_top_10_lb = b.p_top_10_lb,
	pp_top_10_ub = b.p_top_10_ub,
	pp_top_50 = b.p_top_50_avg,
	pp_top_50_lb = b.p_top_50_lb,
	pp_top_50_ub = b.p_top_50_ub
from #university_main_field_period2 as a
left join #university_main_field_period_indicators_frac as b on a.university_main_field_begin_year_no = b.university_main_field_begin_year_no

alter table university_main_field_period_impact_indicators add constraint pk_university_main_field_period_impact_indicators primary key(university_id, main_field_id, period_begin_year, fractional_counting)



-- Create university_main_field_period_collab_indicators table. This table contains the collaboration indicators for each university-main_field-period combination.

drop table if exists university_main_field_period_collab_indicators
create table university_main_field_period_collab_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
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

insert into university_main_field_period_collab_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
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
left join #university_main_field_period_indicators_full as b on a.university_main_field_begin_year_no = b.university_main_field_begin_year_no

alter table university_main_field_period_collab_indicators add constraint pk_university_main_field_period_collab_indicators primary key(university_id, main_field_id, period_begin_year)



-- Create university_main_field_period_oa_indicators table. This table contains the open access indicators for each university-main_field-period combination.

drop table if exists university_main_field_period_oa_indicators
create table university_main_field_period_oa_indicators
(
	university_id int not null,
	main_field_id int not null,
	period_begin_year int not null,
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

insert into university_main_field_period_oa_indicators with(tablock)
select
	a.university_id,
	a.main_field_id,
	a.period_begin_year,
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
left join #university_main_field_period_indicators_full as b on a.university_main_field_begin_year_no = b.university_main_field_begin_year_no

alter table university_main_field_period_oa_indicators add constraint pk_university_main_field_period_oa_indicators primary key(university_id, main_field_id, period_begin_year)
