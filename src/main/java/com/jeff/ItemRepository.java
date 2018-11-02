package com.jeff;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    /**
     * 按照价格区间查询
     * @param start
     * @param end
     * @return
     */
    List<Item> findByPriceBetween(double start, double end);


}
