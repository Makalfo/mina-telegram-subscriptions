CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT,
    telegram_name TEXT,
    telegram_first TEXT,
    public_key TEXT
    );

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT,
    telegram_name TEXT,
    telegram_first TEXT,
    public_key TEXT
    );