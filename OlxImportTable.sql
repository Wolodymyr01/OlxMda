-- Create or clear olx_house_price table for import

IF OBJECT_ID('[dbo].[olx_house_price]', 'U') IS NOT NULL
BEGIN
    TRUNCATE TABLE [dbo].[olx_house_price]
    PRINT 'Table [dbo].[olx_house_price] exists. Data cleared.'
END
ELSE
BEGIN
    CREATE TABLE [dbo].[olx_house_price](
        [price] [float] NOT NULL,
        [price_per_meter] [float] NOT NULL,
        [offer_type] [nvarchar](50) NOT NULL,
        [floor] [tinyint] NULL,
        [area] [float] NULL,
        [rooms] [tinyint] NOT NULL,
        [offer_type_of_building] [nvarchar](50) NULL,
        [market] [nvarchar](50) NOT NULL,
        [city_name] [nvarchar](50) NOT NULL,
        [voivodeship] [nvarchar](50) NOT NULL,
        [month] [nvarchar](50) NOT NULL,
        [year] [smallint] NOT NULL,
        [population] [int] NOT NULL,
        [longitude] [float] NOT NULL,
        [latitude] [float] NOT NULL
    )
    PRINT 'Table [dbo].[olx_house_price] created.'
END

GO
