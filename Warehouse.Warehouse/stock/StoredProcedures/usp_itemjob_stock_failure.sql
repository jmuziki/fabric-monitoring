CREATE   PROCEDURE stock.usp_itemjob_stock_failure
AS
BEGIN
    SET NOCOUNT ON;
    THROW 51000, 'Intentional stock failure for item job logs', 1;
END;