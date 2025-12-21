set language English;

if not exists (select 1 from DimMarket)
insert into DimMarket(MarketLabel)
select distinct market from olx_house_price

if not exists (select 1 from DimOffer)
insert into DimOffer(OfferType)
select distinct offer_type from olx_house_price

if not exists (select 1 from DimProperty)
with CleanProperty as (
	select
        offer_type_of_building as PropertyType,
	    floor,
	    rooms,
	    case
		    when area is null then null
		    when area > 1500 then cast(area / 100.0 as decimal(12,2))
		    else area
	    end as CleanArea
	from olx_house_price
)
insert into DimProperty(PropertyType, Floor, AreaCategory, AreaCode, RoomsCategory)
-- data set contains invalid formatting missing '.' in area (e.g. 4223 instead if 42.23).
-- To clean data and minimise error, the following transformation is required assuming
-- values between 1500 m2 and 4500 m2 (overllaping band after transformation) are extremely
-- rare cases (outliers) and can be skipped for the purposes of this MVP.
-- For Production purposes these valued require further attention and manual processing.
select distinct 
	PropertyType,
	Floor,
    case
        when CleanArea is null then null
        when CleanArea < 20 then '00-19'
        when CleanArea < 30 then '20-29'
        when CleanArea < 40 then '30-39'
        when CleanArea < 50 then '40-49'
        when CleanArea < 60 then '50-59'
        when CleanArea < 70 then '60-69'
        when CleanArea < 80 then '70-79'
        when CleanArea < 90 then '80-89'
        when CleanArea < 100 then '90-99'
        else '100+'
    end as AreaCategory,
    case
        when CleanArea is null then null
        when CleanArea < 20 then  1
        when CleanArea < 30 then  2
        when CleanArea < 40 then  3
        when CleanArea < 50 then  4
        when CleanArea < 60 then  5
        when CleanArea < 70 then  6
        when CleanArea < 80 then  7
        when CleanArea < 90 then  8
        when CleanArea < 100 then 9
        else 10
    end as AreaCode,
	case
		when rooms < 4 then cast(rooms as nvarchar(2))
		else '4+'
	end as RoomsCategory
from CleanProperty

if not exists (select 1 from DimDate)
with ParsedDate as (
    select distinct [year],
    [month],
    month(convert(date, [month] + ' 18, 2022', 113)) as monthNumeric
    from olx_house_price
)
insert into DimDate([Year], YearMonth, [Month], MonthLabel, Label, [Quarter], QuarterLabel)
select distinct 
    [year],
    [year] * 100 + monthNumeric,
    monthNumeric,
    [month],
    [month] + ' ' + cast([year] as nvarchar(4)),
    case
        when monthNumeric between 1 and 3 then 1
        when monthNumeric between 4 and 6 then 2
        when monthNumeric between 7 and 9 then 3
        else 4
    end,
    concat(
    case
        when monthNumeric between 1 and 3 then 'Q1'
        when monthNumeric between 4 and 6 then 'Q2'
        when monthNumeric between 7 and 9 then 'Q3'
        else 'Q4'
    end, ' ', cast([year] as nvarchar(4)))
from ParsedDate
order by 2

if not exists (select 1 from DimLocation)
begin
    drop table if exists #CityStatusMap
    create table #CityStatusMap
    (
        CityName    nvarchar(200) NOT NULL,
        StatusCode  int           NOT NULL,  -- 1=NationalCapital, 2=Regional capital
        StatusName  nvarchar(50)  NOT NULL,  -- 'NationalCapital' / 'Regional capital'
    );

    insert into #CityStatusMap (CityName, StatusCode, StatusName)
    values
    (N'Warszawa', 1, N'National Capital'),
    (N'Białystok', 2, N'Regional capital'),
    (N'Bydgoszcz', 2, N'Regional capital'),
    (N'Toruń',     2, N'Regional capital'),
    (N'Gdańsk',    2, N'Regional capital'),
    (N'Gorzów Wielkopolski', 2, N'Regional capital'),
    (N'Zielona Góra',        2, N'Regional capital'),
    (N'Katowice',  2, N'Regional capital'),
    (N'Kielce',    2, N'Regional capital'),
    (N'Kraków',    2, N'Regional capital'),
    (N'Lublin',    2, N'Regional capital'),
    (N'Łódź',      2, N'Regional capital'),
    (N'Olsztyn',   2, N'Regional capital'),
    (N'Opole',     2, N'Regional capital'),
    (N'Poznań',    2, N'Regional capital'),
    (N'Rzeszów',   2, N'Regional capital'),
    (N'Szczecin',  2, N'Regional capital'),
    (N'Wrocław',   2, N'Regional capital');

    create unique index IXC_CityStatusMap_CityName
    on #CityStatusMap (CityName);

    insert into DimLocation(Country, City, Region, Longtitude, Latitude, Population, StatusCode, StatusLabel)
    select distinct
        'Poland',
        city_name,
        voivodeship,
        longitude,
        latitude,
        population,
        coalesce(csm.StatusCode, 3),
        coalesce(csm.StatusName, N'Small town')
    from olx_house_price ohp
    left join #CityStatusMap csm on ohp.city_name collate Polish_100_CI_AI = csm.CityName collate Polish_100_CI_AI

    drop table #CityStatusMap
end

if not exists (select 1 from FactOfferSnapshot)
with CleanProperty as (
	select
        offer_type_of_building,
        area,
        price,
	    floor,
        city_name,
        market,
        offer_type,
        year,
        month,
	    rooms,
	    case
		    when area is null then null
		    when area > 1500 then cast(area / 100.0 as decimal(12,2))
		    else area
	    end as clean_area
	from olx_house_price
)
insert into FactOfferSnapshot(OfferKey, MarketKey, DateKey, LocationKey, PropertyKey, Area, Price)
select distinct
    o.OfferKey,
    m.MarketKey,
    d.DateKey,
    l.LocationKey,
    p.PropertyKey,
    olx.clean_area,
    olx.price
from CleanProperty olx
join DimDate d on d.Year = olx.year and d.MonthLabel = olx.month
join DimLocation l on l.City = olx.city_name
join DimMarket m on m.MarketLabel = olx.market
join DimOffer o on o.OfferType = olx.offer_type
join DimProperty p on isnull(p.PropertyType, 'Unknown') = isnull(olx.offer_type_of_building, 'Unknown') 
    and isnull(p.Floor, 100) = isnull(olx.floor, 100)
    and case
            when p.RoomsCategory = '4+' then 4
            else cast(p.RoomsCategory as int)
        end = olx.rooms
    and case
            when clean_area is null then -1
            when clean_area < 20 then  1
            when clean_area < 30 then  2
            when clean_area < 40 then  3
            when clean_area < 50 then  4
            when clean_area < 60 then  5
            when clean_area < 70 then  6
            when clean_area < 80 then  7
            when clean_area < 90 then  8
            when clean_area < 100 then 9
            else 10
        end = isnull(p.AreaCode, -1)
        order by 6 desc