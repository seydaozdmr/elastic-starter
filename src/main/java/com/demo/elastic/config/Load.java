package com.demo.elastic.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Load implements CommandLineRunner {
    private final ElasticCon elasticCon;

    public Load(ElasticCon elasticCon) {
        this.elasticCon = elasticCon;
    }

    @Override
    public void run(String... args) throws Exception {
        ElasticsearchClient elasticsearchClient = elasticCon.createClient();
        elasticCon.addIndices(elasticsearchClient);

    }
}
