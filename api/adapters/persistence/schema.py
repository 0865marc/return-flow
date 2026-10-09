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
)
