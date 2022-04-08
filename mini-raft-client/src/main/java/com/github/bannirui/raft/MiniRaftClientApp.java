package com.github.bannirui.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@SpringBootApplication
public class MiniRaftClientApp
{
    public static void main(String[] args)
    {
        SpringApplication.run(MiniRaftClientApp.class, args);
    }
}
