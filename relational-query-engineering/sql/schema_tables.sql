-- =========================================================
-- Relational Query Engineering
-- Schema Definition (Tables)
-- PostgreSQL
-- =========================================================

-- Drop existing tables if present (safe re-runs)
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS submissions;
DROP TABLE IF EXISTS subreddits;
DROP TABLE IF EXISTS authors;

-- ---------------------------------------------------------
-- Authors
-- ---------------------------------------------------------
CREATE TABLE authors (
    author_id      BIGINT PRIMARY KEY,
    author_name    TEXT NOT NULL,
    created_utc    TIMESTAMP
);

-- ---------------------------------------------------------
-- Subreddits
-- ---------------------------------------------------------
CREATE TABLE subreddits (
    subreddit_id   BIGINT PRIMARY KEY,
    display_name   TEXT NOT NULL,
    title          TEXT,
    created_utc    TIMESTAMP,
    subscribers    BIGINT
);

-- ---------------------------------------------------------
-- Submissions
-- ---------------------------------------------------------
CREATE TABLE submissions (
    submission_id  BIGINT PRIMARY KEY,
    author_id      BIGINT NOT NULL,
    subreddit_id   BIGINT NOT NULL,
    title          TEXT,
    score          INTEGER,
    created_utc    TIMESTAMP,
    num_comments   INTEGER
);

-- ---------------------------------------------------------
-- Comments
-- ---------------------------------------------------------
CREATE TABLE comments (
    comment_id     BIGINT PRIMARY KEY,
    submission_id  BIGINT NOT NULL,
    author_id      BIGINT NOT NULL,
    body           TEXT,
    score          INTEGER,
    created_utc    TIMESTAMP
);
