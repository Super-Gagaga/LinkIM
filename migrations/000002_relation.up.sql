-- 关系库：好友（双向冗余插入）、群、群成员（设计文档 9.2 DDL）
CREATE TABLE friend (
  uid        BIGINT      NOT NULL,
  friend_uid BIGINT      NOT NULL,
  status     TINYINT     NOT NULL DEFAULT 0,
  remark     VARCHAR(64) NOT NULL DEFAULT '',
  PRIMARY KEY (uid, friend_uid),
  KEY idx_friend (friend_uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `group` (
  id          BIGINT      NOT NULL,
  name        VARCHAR(64) NOT NULL,
  owner_uid   BIGINT      NOT NULL,
  max_members INT         NOT NULL DEFAULT 500,
  created_at  DATETIME    NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE group_member (
  group_id BIGINT   NOT NULL,
  uid      BIGINT   NOT NULL,
  role     TINYINT  NOT NULL DEFAULT 0,
  join_at  DATETIME NOT NULL,
  PRIMARY KEY (group_id, uid),
  KEY idx_uid (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
