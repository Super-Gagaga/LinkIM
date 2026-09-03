-- 会话表（每用户一份，分表键 uid；设计文档 9.2 DDL 原样）
CREATE TABLE conversation (
  uid        BIGINT      NOT NULL,
  conv_id    VARCHAR(64) NOT NULL,
  conv_type  TINYINT     NOT NULL,
  target_id  BIGINT      NOT NULL,
  last_seq   BIGINT      NOT NULL DEFAULT 0,
  read_seq   BIGINT      NOT NULL DEFAULT 0,
  unread     INT         NOT NULL DEFAULT 0,
  top        TINYINT     NOT NULL DEFAULT 0,
  muted      TINYINT     NOT NULL DEFAULT 0,
  updated_at DATETIME    NOT NULL,
  PRIMARY KEY (uid, conv_id),
  KEY idx_uid_updated (uid, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
