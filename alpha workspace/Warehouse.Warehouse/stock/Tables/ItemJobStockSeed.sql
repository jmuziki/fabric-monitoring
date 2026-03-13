CREATE TABLE [stock].[ItemJobStockSeed] (

	[SeedId] bigint NOT NULL, 
	[Ticker] varchar(16) NOT NULL, 
	[Price] decimal(18,4) NOT NULL, 
	[EventTs] datetime2(6) NOT NULL, 
	[UpdatedTs] datetime2(6) NULL
);