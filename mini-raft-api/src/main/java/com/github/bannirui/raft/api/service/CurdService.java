package com.github.bannirui.raft.api.service;


import com.github.bannirui.raft.api.bean.proto.CurdProto;

/**
 * <p>系统整合raft之后开放的功能</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface CurdService
{
    /**
     * <p>存</p>
     * @since 2022/4/9
     * @author dingrui
     * @param request:
     * @return com.github.bannirui.raft.api.bean.proto.CurdProto.SetResponse
     */
    CurdProto.SetResponse put(CurdProto.SetRequest request);

    /**
     * <p>取</p>
     * @since 2022/4/9
     * @author dingrui
     * @param request:
     * @return com.github.bannirui.raft.api.bean.proto.CurdProto.GetResponse
     */
    CurdProto.GetResponse get(CurdProto.GetRequest request);
}
