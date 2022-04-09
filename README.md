# mini-raft

raft implementation 8 java

参考[raft-java](https://gitee.com/wenweihu86/raft-java.git)

### todo

* [X]  leader election
* [X]  log replication
* [X]  safety

### 依赖

* jdk11
* springboot
* brpc
* rocksdb

### 测试

1. 启动服务端`cd mini-raft-server/deply && sh ./deploy.sh`
2. 启动客户端 mini-raft-client

   ```java
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
           String key = "hello00";
           MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/client/get").contentType(MediaType.APPLICATION_JSON).param("key", key)).andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print()).andReturn();
       }

       @Test
       public void putOp() throws Exception
       {
           String key = "hello00";
           String value = "world00";
           MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/client/put").contentType(MediaType.APPLICATION_JSON).param("key", key).param("value", value)).andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print()).andReturn();
       }
   }
   ```

### 目录结构

```shell
mini-raft
|--- mini-raft-api
|--- mini-raft-bean
|--- mini-raft-client   // 调用存取接口
|--- mini-raft-common
|--- mini-raft-core     // raft实现
|--- mini-raft-server   // 系统整合raft 服务端 提供数据存取实现
|___ README.md
```
