package com.github.bannirui.raft.controller;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class CurdControllerTest extends TestCase
{

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void before()
    {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void getOp() throws Exception
    {
        String key = "hello01";
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/client/get").contentType(MediaType.APPLICATION_JSON).param("key", key)).andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print()).andReturn();
    }

    @Test
    public void putOp() throws Exception
    {
        String key = "hello01";
        String value = "world01";
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/client/put").contentType(MediaType.APPLICATION_JSON).param("key", key).param("value", value)).andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print()).andReturn();
    }
}