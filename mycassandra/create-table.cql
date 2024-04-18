CREATE KEYSPACE IF NOT EXISTS some
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS some.members (
    id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    gender TEXT,
    ip_address TEXT,
    cc TEXT,
    country TEXT,
    birthdate TEXT,
    salary DOUBLE,
    title TEXT,
    comments TEXT,
    PRIMARY KEY (id)
);