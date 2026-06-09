CREATE TABLE IF NOT EXISTS public.workqueue (
    id SERIAL PRIMARY KEY,
    timecreatedutc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel VARCHAR(500) NOT NULL,
    payload TEXT NOT NULL,
    timeprocessedutc TIMESTAMP NULL,
    failedat TIMESTAMP NULL,
    retrycount INT NOT NULL DEFAULT 0
);

-- Safe upgrade from prior schema versions
ALTER TABLE public.workqueue ADD COLUMN IF NOT EXISTS failedat TIMESTAMP NULL;
ALTER TABLE public.workqueue ADD COLUMN IF NOT EXISTS retrycount INT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workqueue'
          AND column_name = 'channel'
          AND character_maximum_length < 500
    ) THEN
        ALTER TABLE public.workqueue ALTER COLUMN channel TYPE VARCHAR(500);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_workqueue_channel_unprocessed
    ON public.workqueue (channel, timecreatedutc)
    WHERE timeprocessedutc IS NULL AND failedat IS NULL;
