-- 用户表（设计文档 9.1：uid 雪花分配，username 唯一）
CREATE TABLE `user` (
  id            BIGINT       NOT NULL,
  username      VARCHAR(64)  NOT NULL,
  password_hash VARCHAR(100) NOT NULL,
  nickname      VARCHAR(64)  NOT NULL DEFAULT '',
  created_at    DATETIME     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
