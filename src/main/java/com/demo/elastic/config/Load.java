package com.demo.elastic.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.demo.elastic.util.ElasticFeedUtil;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Load implements CommandLineRunner {
    private final ElasticCon elasticCon;
    private final ElasticFeedUtil elasticFeedUtil;

    public Load(ElasticCon elasticCon, ElasticFeedUtil elasticFeedUtil) {
        this.elasticCon = elasticCon;
        this.elasticFeedUtil = elasticFeedUtil;
    }

    @Override
    public void run(String... args) throws Exception {
      //  ElasticsearchClient elasticsearchClient = elasticCon.createClient();
//        elasticCon.addIndices(elasticsearchClient);

        //elasticFeedUtil.loadProduct();
        //elasticFeedUtil.getProduct();
        elasticFeedUtil.findProduct("bike");
        //elasticFeedUtil.updateProduct();
        elasticFeedUtil.updateDocumentFields();


    }
}
