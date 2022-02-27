package cn.itcast.hotel;


import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocTest {

    @Autowired
    IHotelService service;

    private RestHighLevelClient client;


    @Test
    void addDoc() throws IOException {

        Hotel hotel = service.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void  getDoc() throws IOException {
        GetRequest request = new GetRequest("hotel", "61083");
        GetResponse res = client.get(request, RequestOptions.DEFAULT);
        String json = res.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void  getDocAll() throws IOException {
//        GetRequest request = new GetRequest("hotel");
        SearchRequest request1 = new SearchRequest("hotel");
        SearchResponse res = client.search(request1, RequestOptions.DEFAULT);
        SearchHit[] hits = res.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    void testUpdateDoc() throws IOException {

        UpdateRequest request = new UpdateRequest("hotel", "61083");

        request.doc(
                "price","952",
                "score","35"
        );

        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDoc() throws IOException {

        DeleteRequest re = new DeleteRequest("hotel", "61083");
        client.delete(re, RequestOptions.DEFAULT);
    }


    @Test
    void testBulk() throws IOException {
        List<Hotel> hotels = service.list();

        BulkRequest request = new BulkRequest();
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotel.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        client.bulk(request, RequestOptions.DEFAULT);
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
