CREATE OR ALTER PROCEDURE [dbo].[workqueue_enqueue]
    @p_channel NVARCHAR(500),
    @p_payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [dbo].[workqueue] ([channel], [payload])
    VALUES (@p_channel, @p_payload);
END
GO
