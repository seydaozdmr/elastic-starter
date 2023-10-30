package com.demo.elastic.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
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
            SearchResponse<Product> search = esClient.search(s -> s
                            .index("products")
                            .query(q -> q
                                    .match(t -> t
                                            .field("category")
                                            .query(searchText)
                                    )
                            ),
                    Product.class
            );

            for (Hit<Product> hit: search.hits().hits()) {
                processProduct(hit.source());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processProduct(Product product){
        LOGGER.info("product : " + product);
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
        Product product = new Product("bk-1", "City bike", 250.0,10);
        UpdateResponse<Product> response;
        try {
            response = esClient.update(u -> u
                            .index("products")
                            .id("bk-1")
                            .doc(product)
                            .upsert(product)
                            .docAsUpsert(true),
                    Product.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("updateResponse : "+response);

    }

    public void deleteDocument(){
        try {
            esClient.delete(d -> d.index("products").id("bk-1"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteIndex(){
        try {
            esClient.indices().delete(c -> c
                    .index("products")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void buildTermQuery(String value){
        Query query = new Query.Builder().term(e->e.field("category").value(value)).build();
        try {
            SearchResponse<Product> search = esClient.search(s -> s
                            .index("products").query(query),
                    Product.class
            );
            for (Hit<Product> hit: search.hits().hits()) {
                processProduct(hit.source());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
