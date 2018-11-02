package com.jeff;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.System.out;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticsearchSpringOrderTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private OrderRepository  orderRepository;

    /**
     * 创建索引库
     */
    @Test
    public void creatIndex() {

        elasticsearchTemplate.createIndex(Order.class);
        elasticsearchTemplate.putMapping(Order.class);

    }




    /**
     * 批量新增
     */
    @Test
    public void addDocuments() {
        List<Order> list = new ArrayList<>();
        list.add(new Order(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg", 100.00));
        list.add(new Order(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg", 200.00));
        list.add(new Order(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg", 300.00));
        list.add(new Order(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg", 400.00));
        list.add(new Order(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg",500.00));
        this.orderRepository.saveAll(list);
    }

    /**
     * 查询所有
     */
    @Test
    public void findAll() {
        Iterable<Order> itemList = orderRepository.findAll();
        itemList.forEach(System.out::println);
    }

    /**
     * 自定义查询
     */
    @Test
    public void findByPriceBetween() {
        List<Order> itemList = this.orderRepository.findByPaymentAmountBetween(300, 500);
        itemList.forEach(System.out::println);
    }

    @Test
    public void findByTitlelike(){
        //词条查询
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        Iterable orderList = this.orderRepository.search(queryBuilder);
        orderList.forEach(System.out::println);
    }
}
