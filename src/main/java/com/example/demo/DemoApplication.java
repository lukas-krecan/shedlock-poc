package com.example.demo;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SchedulerLock;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.ScheduledLockConfiguration;
import net.javacrumbs.shedlock.spring.ScheduledLockConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.Duration;

@SpringBootApplication
@EnableScheduling
public class DemoApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DemoApplication.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;


    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS shedlock(name VARCHAR(64), lock_until TIMESTAMP(3), locked_at TIMESTAMP(3), locked_by  VARCHAR(255), PRIMARY KEY (name))");
    }

    @Configuration
    public static class Conf {
        @Bean
        LockProvider lockProvider(DataSource dataSource) {
            return new JdbcTemplateLockProvider(dataSource);
        }

        @Bean
        public ScheduledLockConfiguration shedLockConfig(
                LockProvider lockProvider) {
            return ScheduledLockConfigurationBuilder
                    .withLockProvider(lockProvider)
                    .withTaskScheduler(new ConcurrentTaskScheduler())
                    .withDefaultLockAtMostFor(Duration.ofMinutes(10))
                    .build();
        }
    }


    @Service
    public class ScheduledService {

        @Scheduled(cron = "0/5 * * * * *")
        @SchedulerLock(name = "testThis")
        public void callSomething() {
            jdbcTemplate
                    .queryForList("select * from shedlock")
                    .forEach(r -> r
                            .entrySet()
                            .forEach(e -> logger.error("entry [{}={}]", e.getKey(), e.getValue())));
            logger.error("Something called");
        }
    }
}
