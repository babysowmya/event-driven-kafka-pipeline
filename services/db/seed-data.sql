-- Drop table if it exists to ensure clean state on rebuild
DROP TABLE IF EXISTS daily_user_item_summary;

-- Create the core aggregation table
CREATE TABLE daily_user_item_summary (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    item_id UUID NOT NULL,
    date DATE NOT NULL,
    view_count INTEGER DEFAULT 0,
    click_count INTEGER DEFAULT 0,
    purchase_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Crucial constraint for idempotency:
    -- A user can only have ONE record per item per day.
    UNIQUE (user_id, item_id, date)
);

-- Helpful index for the API's most common query pattern.
CREATE INDEX idx_daily_summary_user_id ON daily_user_item_summary (user_id);

-- Insert a seed record so the API can be tested immediately
-- without waiting for Kafka events to process.
INSERT INTO daily_user_item_summary
    (id, user_id, item_id, date, view_count, click_count, purchase_count)
VALUES
    ('11111111-1111-1111-1111-111111111111',
     '22222222-2222-2222-2222-222222222222',
     '33333333-3333-3333-3333-333333333333',
     CURRENT_DATE, 5, 2, 1);