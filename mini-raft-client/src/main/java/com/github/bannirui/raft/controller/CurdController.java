package com.github.bannirui.raft.controller;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.github.bannirui.raft.api.bean.proto.CurdProto;
import com.github.bannirui.raft.api.service.CurdService;
import com.googlecode.protobuf.format.JsonFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@RestController
@RequestMapping("/client")
public class CurdController
{
    @Value("${app.cluster}")
    private String cluster;

    private final JsonFormat jsonFormat = new JsonFormat();

    @GetMapping("/get")
    public String getOp(String key)
    {
        RpcClientOptions rpcClientOptions = new RpcClientOptions();
        rpcClientOptions.setConnectTimeoutMillis(10_000);
        rpcClientOptions.setReadTimeoutMillis(10_000);
        rpcClientOptions.setWriteTimeoutMillis(10_000);
        RpcClient rpcClient = new RpcClient(this.cluster, rpcClientOptions);
        CurdService curdService = BrpcProxy.getProxy(rpcClient, CurdService.class);
        CurdProto.GetRequest request = CurdProto.GetRequest.newBuilder().setKey(key).build();
        CurdProto.GetResponse response = curdService.get(request);
        rpcClient.stop();
        return this.jsonFormat.printToString(response);
    }

    @GetMapping("/put")
    public String setOp(String key, String value)
    {
        RpcClientOptions rpcClientOptions = new RpcClientOptions();
        rpcClientOptions.setConnectTimeoutMillis(10_000);
        rpcClientOptions.setReadTimeoutMillis(10_000);
        rpcClientOptions.setWriteTimeoutMillis(10_000);
        RpcClient rpcClient = new RpcClient(this.cluster, rpcClientOptions);
        CurdService curdService = BrpcProxy.getProxy(rpcClient, CurdService.class);
        CurdProto.SetRequest request = CurdProto.SetRequest.newBuilder().setKey(key).setValue(value).build();
        CurdProto.SetResponse response = curdService.put(request);
        rpcClient.stop();
        return this.jsonFormat.printToString(response);
    }
}
