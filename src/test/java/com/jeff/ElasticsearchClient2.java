package com.jeff;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.*;

/**
 * Created by zhangying on 2018/10/31.
 */
public class ElasticsearchClient2 {


    public final static String HOST = "192.168.221.78";

    public final static int PORT = 9300; //http请求的端口是9200，客户端是9300

    private TransportClient client = null;
    /**
     * 获取客户端连接信息
     * @Title: getConnect
     * @author sunt
     * @date 2017年11月23日
     * @return void
     * @throws UnknownHostException
     */
    @SuppressWarnings({ "resource", "unchecked" })
    @Before
    public void getConnect() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName(HOST),PORT));
      //  logger.info("连接信息:" + client.toString());
    }

    /**
     * 关闭连接
     * @Title: closeConnect
     * @author sunt
     * @date 2017年11月23日
     * @return void
     */
    @After
    public void closeConnect() {
        if(null != client) {
         //   logger.info("执行关闭连接操作...");
            client.close();
        }
    }

    /**
     * 手动生成JSON
     */
    @Test
    public void createJSON(){

        String json = "{" +
                "\"user\":\"fendo\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"Hell word\"" +
                "}";

        IndexResponse response = client.prepareIndex("fendo", "fendodate")
                .setSource(json)
                .get();
        System.out.println(response.getResult());

    }

    /**
     * 使用集合
     */
    @Test
    public void CreateList(){

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user","kimchy");
        json.put("postDate","2013-01-30");
        json.put("message","trying out Elasticsearch");

        IndexResponse response = client.prepareIndex("fendo", "fendodate")
                .setSource(json)
                .get();
        System.out.println(response.getResult());

    }

    /**
     * 使用JACKSON序列化
     * @throws Exception
     */
    @Test
    public void CreateJACKSON() throws Exception{

        CsdnBlog csdn=new CsdnBlog();
        csdn.setAuthor("fendo");
        csdn.setContent("这是JAVA书籍");
        csdn.setTag("C");
        csdn.setView("100");
        csdn.setTitle("编程");
        csdn.setDate(new Date().toString());

        // instance a json mapper
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        // generate json
        byte[] json = mapper.writeValueAsBytes(csdn);

        System.out.println(json.toString());
        System.out.println("---------------------");
        IndexResponse response = client.prepareIndex("fendo", "fendodate")
                .setSource(json)
                .get();
        System.out.println(response.getResult());
    }

}
