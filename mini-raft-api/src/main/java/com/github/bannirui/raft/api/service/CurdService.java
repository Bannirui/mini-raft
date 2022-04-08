package com.github.bannirui.raft.api.service;


import com.github.bannirui.raft.api.bean.proto.CurdProto;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
public interface CurdService
{
    CurdProto.SetResponse set(CurdProto.SetRequest request);

    CurdProto.GetResponse get(CurdProto.GetRequest request);
}
