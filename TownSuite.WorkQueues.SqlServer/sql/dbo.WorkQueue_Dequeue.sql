CREATE OR ALTER PROCEDURE [dbo].[workqueue_dequeue]
    @p_channel NVARCHAR(500),
    @p_offset  INT,
    @p_payload NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DELETE TOP(1) FROM [dbo].[workqueue]
    OUTPUT deleted.[payload]
    WHERE [id] = (
        SELECT [id]
        FROM [dbo].[workqueue] WITH (ROWLOCK, UPDLOCK, READPAST)
        WHERE [channel] = @p_channel
          AND [failedat] IS NULL
        ORDER BY [id]
        OFFSET @p_offset ROWS
        FETCH NEXT 1 ROWS ONLY
    );
END
GO
