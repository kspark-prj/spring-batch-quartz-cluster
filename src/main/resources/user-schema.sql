-- =============================================================================
-- Spring Security User Schema & Initial Admin User (PostgreSQL)
-- 비밀번호 'admin1!' 의 BCrypt 해시: $2a$10$8.UnVuG9HHgffUDAlk8qfOUVGkqRzgVymGe07xd00DMxs.AQubh4a
-- =============================================================================

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'ROLE_ADMIN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 초기 관리자 계정 생성 (아이디: admin, 비밀번호: admin1!)
INSERT INTO users (username, password, role, created_at)
VALUES ('admin', '$2a$10$/JWK.7XMc.xNnLi9b0sWteU3v.MnAi6Bd.GYiGXSUsIukbIVMBqpe', 'ROLE_ADMIN', CURRENT_TIMESTAMP)
ON CONFLICT (username) DO NOTHING;
