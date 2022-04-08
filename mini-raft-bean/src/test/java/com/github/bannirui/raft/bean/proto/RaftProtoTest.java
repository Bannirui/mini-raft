package com.github.bannirui.raft.bean.proto;


import org.junit.Test;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
public class RaftProtoTest
{

    @Test
    public void testProtoc(){
        RaftProto.Endpoint e = RaftProto.Endpoint.newBuilder().setHost("127.0.0.1").setPort(8001).build();
        System.out.println(e);
    }
}