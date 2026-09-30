package com.example.demo.security.init;

import com.example.demo.security.entity.User;
import com.example.demo.security.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Override
    public void run(String... args) throws Exception {
        Optional<User> existingUser = userRepository.findByUsername("admin");
        
        if (existingUser.isEmpty()) {
            String encodedPw = passwordEncoder.encode("admin1!");
            User admin = User.builder()
                    .username("admin")
                    .password(encodedPw)
                    .role("ROLE_ADMIN")
                    .build();

            userRepository.save(admin);
            log.info("==================================================");
            log.info("초기 관리자 계정이 새로 생성되었습니다. (ID: admin, PW: admin1!)");
            log.info("BCrypt Hash: {}", encodedPw);
            log.info("==================================================");
        } else {
            User admin = existingUser.get();
            if (!passwordEncoder.matches("admin1!", admin.getPassword())) {
                String encodedPw = passwordEncoder.encode("admin1!");
                admin.setPassword(encodedPw);
                userRepository.save(admin);
                log.info("==================================================");
                log.info("기존 관리자 계정(admin)의 비밀번호를 'admin1!'의 BCrypt 해시로 업데이트했습니다.");
                log.info("BCrypt Hash: {}", encodedPw);
                log.info("==================================================");
            } else {
                log.info("==================================================");
                log.info("관리자 계정(admin)이 이미 정상적인 비밀번호로 존재합니다.");
                log.info("==================================================");
            }
        }
    }
}
