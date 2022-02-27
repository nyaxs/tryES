package cn.itcast.hotel;


import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelSearchTest {

    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        //1 prepare request
        SearchRequest request = new SearchRequest("hotel");
        //2 prepare dsl
        request.source().size(20);
        request.source().query(QueryBuilders.matchAllQuery());
        //3 send request
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        Long total = hits.getTotalHits().value;
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            System.out.println(hit.getIndex()+hotelDoc);

        }
    }

    @Test
    void testMatch() throws IOException {
        //1 prepare request
        SearchRequest request = new SearchRequest("hotel");
        //2 prepare dsl
        request.source().size(20);
        request.source().query(QueryBuilders.matchQuery("all", "如家"));

        //3 send request
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        getHitsData(response);
    }

    @Test
    void testBool() throws IOException {
        //1 prepare request
        SearchRequest request = new SearchRequest("hotel");
        //2 prepare dsl
        request.source().size(20);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("city", "杭州"));
//        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQueryBuilder);

        //3 send request
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        getHitsData(response);
    }

    private void getHitsData(SearchResponse response) {
        SearchHits hits = response.getHits();
        Long total = hits.getTotalHits().value;
        System.out.printf("共查询到%d条数据\n", total);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            System.out.println(hit.getIndex()+hotelDoc);

        }
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://124.222.61.69:9200")));
    }


    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }







    @Test
    void testInit() {
        System.out.print(client);
    }

    @Test
    void createHotelIndices() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest("hotel");

        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistsIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
}
