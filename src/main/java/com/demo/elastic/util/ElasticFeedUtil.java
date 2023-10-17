package com.demo.elastic.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.demo.elastic.model.Product;
import org.springframework.stereotype.Component;
import java.util.logging.Logger;

import java.io.IOException;

@Component
public class ElasticFeedUtil {
    private final ElasticsearchClient esClient;
    private static Logger LOGGER = Logger.getLogger("ElasticFeedUtil.class");

    public ElasticFeedUtil(ElasticsearchClient esClient) {
        this.esClient = esClient;
    }

    public void loadProduct(){
        Product product = new Product("bk-1", "City bike", 123.0);

        IndexResponse response = null;
        try {
            response = esClient.index(i -> i
                    .index("products")
                    .id(product.getName())
                    .document(product)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Indexed with version " + response.version());
    }

    public void getProduct(){
        GetResponse<Product> response = null;
        try {
            response = esClient.get(g -> g
                            .index("products")
                            .id("bk-1"),
                    Product.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (response.found()) {
            Product product = response.source();
            LOGGER.info("Product name " + product.getName());
        } else {
            LOGGER.info ("Product not found");
        }
    }

    public void findProduct(String searchText){
        try {
            SearchResponse<Product> response = esClient.search(s -> s
                            .index("products")
                            .query(q -> q
                                    .match(t -> t
                                            .field("category")
                                            .query(searchText)
                                    )
                            ),
                    Product.class
            );

            LOGGER.info(response.hits().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateProduct(){
        Product product = new Product("bk-1", "City bike", 200.0);

        try {
            esClient.update(u -> u
                            .index("products")
                            .id("bk-1")
                            .doc(product),
                    Product.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateDocumentFields(){

        Product product = new Product("bk-1", "City bike", 123.0,10);

        try {
            esClient.update(u -> u
                            .index("products")
                            .id("bk-1")
                            .upsert(product),
                    Product.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
