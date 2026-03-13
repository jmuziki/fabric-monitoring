CREATE   PROCEDURE stock.usp_itemjob_stock_daily
AS
BEGIN
    SET NOCOUNT ON;
    EXEC stock.usp_itemjob_stock_success;

    BEGIN TRY
        EXEC stock.usp_itemjob_stock_failure;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
    END CATCH;
END;