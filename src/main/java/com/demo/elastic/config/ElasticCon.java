package com.demo.elastic.config;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;

@Component
public class ElasticCon {

    private final String SERVER_URL = "https://localhost:9200";

    private final String API_KEY= "ZjBuME9Jc0JJTVBtQkNyU1dsYzY6dmNQN2ZwUVpRZjJoT2l5dHQ3bGEwZw==";



    @Bean
    public ElasticsearchClient esClient(){
        //Brand new RestClient
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

//    @Bean
//    public ElasticsearchClient esClient(){
//        final String fingerprint = "c4c2389ec21bde761031b28590d5d8cfd6736a2e8275fa0d3b95c6a25fd1e08c";
//
//        SSLContext sslContext = TransportUtils
//                .sslContextFromCaFingerprint(fingerprint);
//
//        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
//        credsProv.setCredentials(
//                AuthScope.ANY, new UsernamePasswordCredentials("Username", "ksAf5xV1*fi+f3E3ioDX")
//        );
//
//        RestClient restClient = RestClient
//                .builder(new HttpHost("localhost", 9200, "https"))
//                .setHttpClientConfigCallback(hc -> hc
//                        .setSSLContext(sslContext)
//                        .setDefaultCredentialsProvider(credsProv)
//                )
//                .build();
//
//// Create the transport and the API client
//        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
//        return new ElasticsearchClient(transport);
//    }

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
