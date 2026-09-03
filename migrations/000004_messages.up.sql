-- 消息表 message_00 ~ message_63（分表键 conv_id；设计文档 9.2 DDL。payload 原文为 VARBINARY(65535)，
-- 但加上行内其余列会超过 MySQL 65535 字节行上限，故调整为 65000）
CREATE TABLE message_00 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_01 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_02 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_03 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_04 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_05 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_06 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_07 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_08 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_09 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_10 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_11 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_12 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_13 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_14 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_15 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_16 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_17 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_18 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_19 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_20 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_21 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_22 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_23 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_24 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_25 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_26 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_27 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_28 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_29 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_30 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_31 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_32 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_33 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_34 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_35 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_36 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_37 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_38 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_39 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_40 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_41 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_42 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_43 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_44 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_45 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_46 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_47 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_48 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_49 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_50 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_51 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_52 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_53 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_54 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_55 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_56 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_57 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_58 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_59 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_60 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_61 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_62 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE message_63 (
  id           BIGINT UNSIGNED NOT NULL,
  conv_id      VARCHAR(64)  NOT NULL,
  seq          BIGINT       NOT NULL,
  sender_id    BIGINT       NOT NULL,
  msg_type     SMALLINT     NOT NULL,
  payload      VARBINARY(65000) NOT NULL,
  status       TINYINT      NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_conv_seq (conv_id, seq),
  KEY idx_conv_time (conv_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
