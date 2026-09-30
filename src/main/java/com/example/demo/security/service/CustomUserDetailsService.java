package com.example.demo.security.service;

import com.example.demo.security.entity.User;
import com.example.demo.security.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomUserDetailsService implements UserDetailsService {

    private final UserRepository userRepository;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        if (username == null) {
            throw new UsernameNotFoundException("사용자명이 null입니다.");
        }
        String cleanUsername = username.trim();
        log.debug("로그인 인증 시도 사용자: {}", cleanUsername);

        User user = userRepository.findByUsername(cleanUsername)
                .orElseGet(() -> userRepository.findByUsername(cleanUsername.toLowerCase())
                .orElseThrow(() -> {
                    log.warn("로그인 실패 - 사용자를 찾을 수 없음: {}", cleanUsername);
                    return new UsernameNotFoundException("사용자를 찾을 수 없습니다: " + cleanUsername);
                }));

        String role = user.getRole();
        if (role == null || role.isBlank()) {
            role = "ROLE_ADMIN";
        } else if (!role.startsWith("ROLE_")) {
            role = "ROLE_" + role;
        }

        log.debug("사용자 정보 조회 성공: {}, Role: {}", user.getUsername(), role);

        return new org.springframework.security.core.userdetails.User(
                user.getUsername(),
                user.getPassword(),
                Collections.singletonList(new SimpleGrantedAuthority(role))
        );
    }
}
