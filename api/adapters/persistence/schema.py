SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id UUID PRIMARY KEY,
        status TEXT NOT NULL CHECK (status IN ('pending', 'delivered'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS returns (
        id UUID PRIMARY KEY,
        delivery_id UUID NOT NULL REFERENCES deliveries (id),
        status TEXT NOT NULL CHECK (status IN ('requested', 'completed'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS commands (
        command_id UUID PRIMARY KEY,
        type TEXT NOT NULL,
        payload JSONB NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
        result JSONB,
        error TEXT,
        error_code TEXT,
        created_at TIMESTAMPTZ DEFAULT clock_timestamp(),
        completed_at TIMESTAMPTZ,
        CHECK (
            (status = 'pending' AND result IS NULL AND error IS NULL)
            OR (status = 'completed' AND result IS NOT NULL AND error IS NULL)
            OR (status = 'failed' AND result IS NULL AND error IS NOT NULL)
        )
    )
    """,
    # Add columns without a default first so existing records retain unknown
    # historical timing instead of acquiring the schema upgrade timestamp.
    """
    ALTER TABLE commands
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ
    """,
    """
    ALTER TABLE commands ALTER COLUMN created_at SET DEFAULT clock_timestamp()
    """,
    "ALTER TABLE commands ADD COLUMN IF NOT EXISTS error_code TEXT",
)
