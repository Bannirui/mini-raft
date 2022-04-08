package com.github.bannirui.raft;

import com.github.bannirui.raft.bootstrap.RaftServerBootstrap;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@SpringBootApplication
@RequiredArgsConstructor
public class MiniRaftServerApp implements ApplicationRunner
{

    private final RaftServerBootstrap raftServerBootstrap;

    public static void main(String[] args)
    {
        SpringApplication.run(MiniRaftServerApp.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception
    {
        this.raftServerBootstrap.start();
    }
}
