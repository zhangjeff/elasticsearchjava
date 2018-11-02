package com.jeff;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface OrderRepository extends ElasticsearchRepository<Order, Long> {

    /**
     * 按照价格区间查询
     * @param start
     * @param end
     * @return
     */
    List<Order> findByPaymentAmountBetween(double start, double end);

}
