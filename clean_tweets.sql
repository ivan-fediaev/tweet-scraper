CREATE TABLE elonmusk (
    id INTEGER NOT NULL PRIMARY KEY,
    created_at DATETIME,
    favorite_count INTEGER,
    retweet_count INTEGER,
    text TEXT,
    neg REAL,
    neu REAL,
    pos REAL,
    compound REAL
);

CREATE TABLE cnnbrk (
    id INTEGER NOT NULL PRIMARY KEY,
    created_at DATETIME,
    favorite_count INTEGER,
    retweet_count INTEGER,
    text TEXT,
    neg REAL,
    neu REAL,
    pos REAL,
    compound REAL
);

INSERT INTO elonmusk(id, created_at, favorite_count, retweet_count, text, neg, neu, pos, compound)
VALUES (123, "2021-07-14 04:18:30", 100, 200, "This is a test", 0.5, 1.0, 2.5, -0.5);