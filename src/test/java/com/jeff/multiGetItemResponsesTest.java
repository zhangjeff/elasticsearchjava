package com.jeff;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

/**
 * Created by zhangying on 2018/11/1.
 */
public class multiGetItemResponsesTest {

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
     * 创建索引库
     * @Title: addIndex1
     * @author sunt
     * @date 2017年11月23日
     * @return void
     * 需求:创建一个索引库为：msg消息队列,类型为：tweet,id为1
     * 索引库的名称必须为小写
     * @throws IOException
     */
    @Test
    public void addIndex1() throws IOException {
        IndexResponse response = client.prepareIndex("msg", "tweet", "4").setSource(XContentFactory.jsonBuilder()
                .startObject().field("userName", "张三")
                .field("sendDate", new Date())
                .field("msg", "你好李四")
                .endObject()).get();




        System.out.println("索引名称:" + response.getIndex() + "\n类型:" + response.getType()
                + "\n文档ID:" + response.getId() + "\n当前实例状态:" + response.status()+"\n当前实例版本号:" + response.getVersion());
    }

    @Test
    public void getItemMultiResponse(){
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("msg", "tweet", "1") //一个id的方式
                .add("msg", "tweet", "2", "3", "4") //多个id的方式
             //   .add("another", "type", "foo")  //可以从另外一个索引获取
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) { //迭代返回值
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {      //判断是否存在
                String json = response.getSourceAsString(); //_source 字段
                System.out.println(json);
            }
        }
    }
}
