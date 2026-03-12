CREATE   PROCEDURE stock.usp_itemjob_stock_success
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @seed BIGINT = ABS(CHECKSUM(NEWID()));

    INSERT INTO stock.ItemJobStockSeed (SeedId, Ticker, Price, EventTs, UpdatedTs)
    VALUES
      (@seed, 'MSFT', 400.0000, SYSUTCDATETIME(), NULL),
      (@seed + 1, 'AAPL', 200.0000, SYSUTCDATETIME(), NULL);

    UPDATE stock.ItemJobStockSeed
       SET Price = Price + 1.2500,
           UpdatedTs = SYSUTCDATETIME()
     WHERE SeedId IN (@seed, @seed + 1);

    DELETE FROM stock.ItemJobStockSeed
     WHERE EventTs < DATEADD(DAY, -30, SYSUTCDATETIME());

    SELECT TOP (1) SeedId, Ticker, Price, EventTs, UpdatedTs
      FROM stock.ItemJobStockSeed
     ORDER BY EventTs DESC;
END;