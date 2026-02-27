-- Создаем таблицу videos
CREATE TABLE IF NOT EXISTS videos (
    id VARCHAR(36) PRIMARY KEY,
    creator_id VARCHAR(36) NOT NULL,
    video_created_at TIMESTAMP NOT NULL,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу video_snapshots
CREATE TABLE IF NOT EXISTS video_snapshots (
    id VARCHAR(36) PRIMARY KEY,
    video_id VARCHAR(36) NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    delta_views_count INTEGER DEFAULT 0,
    delta_likes_count INTEGER DEFAULT 0,
    delta_comments_count INTEGER DEFAULT 0,
    delta_reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_delta_views ON video_snapshots(delta_views_count) WHERE delta_views_count > 0;