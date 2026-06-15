CREATE OR ALTER PROCEDURE [dbo].[workqueue_dequeue_nondestructive]
    @p_channel NVARCHAR(500),
    @p_offset  INT,
    @p_payload NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @the_payload NVARCHAR(MAX);

    UPDATE TOP(1) [dbo].[workqueue]
    SET [timeprocessedutc] = GETUTCDATE(),
        @the_payload = [payload]
    WHERE [id] = (
        SELECT [id]
        FROM [dbo].[workqueue] WITH (ROWLOCK, UPDLOCK, READPAST)
        WHERE [channel] = @p_channel
          AND [timeprocessedutc] IS NULL
          AND [failedat] IS NULL
        ORDER BY [id]
        OFFSET @p_offset ROWS
        FETCH NEXT 1 ROWS ONLY
    );

    SELECT @p_payload = @the_payload;
END
GO
