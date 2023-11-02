package com.demo.elastic.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import com.demo.elastic.model.Product;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.logging.Logger;

import java.io.IOException;

@Component
public class ElasticFeedUtil {
    private final ElasticsearchClient esClient;
    private static Logger LOGGER = Logger.getLogger("ElasticFeedUtil.class");

    public ElasticFeedUtil(ElasticsearchClient esClient) {
        this.esClient = esClient;
    }

    /**
     * Product nesnesini elastic indisine eklemek için esClient.index kullanıyoruz. IndexRequest.of(i-> i ..) şeklinde de yapılandırılabilir.
     */
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

    /**
     * Bulk index : birden çok nesneyi toplu olarak eklemek için kullanabiliriz
     */

    public void loadBulkOperations(List<Product> productList){

        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        for(Product product:productList){
            bulkRequest.operations(op ->
                op.index(idx -> idx.index("products")
                        .id(product.getName())
                        .document(product))
            );
        }
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = esClient.bulk(bulkRequest.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (bulkResponse.errors()) {
            LOGGER.warning("Bulk had errors");
            for (BulkResponseItem item: bulkResponse.items()) {
                if (item.error() != null) {
                    LOGGER.warning(item.error().reason());
                }
            }
        }

    }

    /**
     * Id'si bilinen bir dökümanı getirmek için get işlevi kullanılabilir
     */
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

    /**
     * Match Query  -> We will start with the simple text match query, searching for bikes in the products index.
     * @param searchText
     */
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

            TotalHits total = search.hits().total();
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                LOGGER.info("There are " + total.value() + " results");
            } else {
                LOGGER.info("There are more than " + total.value() + " results");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Nested Query with match query and range query
     * @param searchText
     * @param price
     */
    public void findWthNestedQueries (String searchText ,double price){
        Query byName = MatchQuery.of(q-> q.field("name").query(searchText))._toQuery();
        Query byMaxPrice = RangeQuery.of(q->q.field("price").gte(JsonData.of(price)))._toQuery();
        SearchResponse response;
        try {
             response = esClient.search(s -> s.index("products").query(q->q.bool(b->b.must(byName).must(byMaxPrice))),Product.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit: hits) {
            Product product = hit.source();
            LOGGER.info("Found product " + product.getName() + ", score " + hit.score());
        }
    }

    /**
     * Elastic içine bir templete vererek daha sonra kodu değiştirmeden sorguyu değiştirebiliriz.
     */
    public void templatedSearch(){
        try {
            esClient.putScript(r -> r
                    .id("query-script")
                    .script(s -> s
                            .lang("mustache")
                            .source("{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}")
                    ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * In the example below we run an aggregation that creates a price histogram from a product index, for the products whose name match a user-provided text.
     */
    public void simpleAggregation(){
        String searchText = "bike";

        Query query = MatchQuery.of(m -> m
                .field("name")
                .query(searchText)
        )._toQuery();
        SearchResponse<Void> response;
        try {
             response = esClient.search(b -> b
                            .index("products")
                            .size(0)
                            .query(query)
                            .aggregations("price-histogram", a -> a
                                    .histogram(h -> h
                                            .field("price")
                                            .interval(50.0)
                                    )
                            ),
                    Void.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<HistogramBucket> buckets = response.aggregations()
                .get("price-histogram")
                .histogram()
                .buckets().array();

        for (HistogramBucket bucket: buckets) {
            LOGGER.info("There are " + bucket.docCount() +
                    " bikes under " + bucket.key());
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
