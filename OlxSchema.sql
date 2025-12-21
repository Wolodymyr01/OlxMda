if not exists (select 1 from sysobjects where name='DimOffer' and xtype='U')
create table DimOffer
(
	OfferKey int primary key identity(1,1),
	OfferType nvarchar(64) not null -- to differentiate private and agency offers
)
go

if not exists (select 1 from sysobjects where name='DimDate' and xtype='U')
create table DimDate
(
	DateKey int primary key identity(1,1),
	Label nvarchar(32) not null, -- user-friendly date interpretation (e.g. October 2022)
	YearMonth int not null, -- numerical date interpretation to allow ordinal operations (>, <) and month-scaled differences.
	Year int not null,
	Month int not null,
	MonthLabel nvarchar(16) not null,
	Quarter tinyint not null,
	QuarterLabel nchar(7) -- format e.g. Q4 2022
)
go

if not exists (select 1 from sysobjects where name='DimLocation' and xtype='U')
create table DimLocation
(
	LocationKey int primary key identity(1,1),
	Country nvarchar(64), -- Poland only for this data set
	City nvarchar(64) not null,
	Region nvarchar(64) not null,
	Longtitude float not null,
	Latitude float not null,
	Population int not null,
	StatusCode int, -- 1 - Capital, 2 - Region capital, 3 - smaller town or village
	StatusLabel nvarchar(16) not null -- user-friendly visualisation
)
go

if not exists (select 1 from sysobjects where name='DimMarket' and xtype='U')
create table DimMarket
(
	MarketKey int primary key identity(1,1),
	MarketLabel nvarchar(16) not null
)

if not exists (select 1 from sysobjects where name='DimProperty' and xtype='U')
create table DimProperty
(
	PropertyKey int primary key identity(1,1),
	PropertyType nvarchar(64) null,
	Floor tinyint null,
	AreaCategory nvarchar(6) null,
	AreaCode int null,
	RoomsCategory nvarchar(2) not null,
)

if not exists (select 1 from sysobjects where name='FactOfferSnapshot' and xtype='U')
create table FactOfferSnapshot
(
	OfferKey int foreign key references DimOffer(OfferKey) not null,
	MarketKey int foreign key references DimMarket(MarketKey) not null,
	DateKey int foreign key references DimDate(DateKey) not null,
	LocationKey int foreign key references DimLocation(LocationKey) not null,
	PropertyKey int foreign key references DimProperty(PropertyKey) not null,
	Area float null,
	Price float not null
)