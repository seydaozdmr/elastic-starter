package com.demo.elastic.config;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ElasticCon {

    private final String SERVER_URL = "https://localhost:9200";

    private final String API_KEY= "ZjBuME9Jc0JJTVBtQkNyU1dsYzY6dmNQN2ZwUVpRZjJoT2l5dHQ3bGEwZw==";



    public ElasticsearchClient createClient(){
        RestClient restClient = RestClient
                .builder(HttpHost.create(SERVER_URL))
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + API_KEY)
                })
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        return new ElasticsearchClient(transport);
    }

   public void addIndices(ElasticsearchClient esClient) {
        try {
            esClient.indices().create(c -> c
                    .index("products")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
