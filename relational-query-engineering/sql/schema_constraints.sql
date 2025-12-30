-- =========================================================
-- Relational Query Engineering
-- Schema Constraints + Indexes
-- PostgreSQL
-- =========================================================

-- Foreign key constraints
ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_author
  FOREIGN KEY (author_id) REFERENCES authors(author_id)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_subreddit
  FOREIGN KEY (subreddit_id) REFERENCES subreddits(subreddit_id)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_submission
  FOREIGN KEY (submission_id) REFERENCES submissions(submission_id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_author
  FOREIGN KEY (author_id) REFERENCES authors(author_id)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

-- Optional data quality constraints (safe, not overly strict)
ALTER TABLE subreddits
  ADD CONSTRAINT chk_subscribers_nonnegative
  CHECK (subscribers IS NULL OR subscribers >= 0);

ALTER TABLE submissions
  ADD CONSTRAINT chk_submission_counts_nonnegative
  CHECK ((score IS NULL OR score >= 0) AND (num_comments IS NULL OR num_comments >= 0));

ALTER TABLE comments
  ADD CONSTRAINT chk_comment_score_nonnegative
  CHECK (score IS NULL OR score >= 0);

-- Performance indexes (typical join + filter patterns)
CREATE INDEX IF NOT EXISTS idx_submissions_author_id   ON submissions(author_id);
CREATE INDEX IF NOT EXISTS idx_submissions_subreddit_id ON submissions(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_submissions_created_utc ON submissions(created_utc);

CREATE INDEX IF NOT EXISTS idx_comments_submission_id  ON comments(submission_id);
CREATE INDEX IF NOT EXISTS idx_comments_author_id      ON comments(author_id);
CREATE INDEX IF NOT EXISTS idx_comments_created_utc    ON comments(created_utc);

-- Useful for lookups/search
CREATE INDEX IF NOT EXISTS idx_subreddits_display_name ON subreddits(display_name);
