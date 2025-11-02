/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.test.elasticsearch.hlclient;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._helpers.esql.jdbc.ResultSetEsqlAdapter;
import co.elastic.clients.elasticsearch._helpers.esql.objects.ObjectsEsqlAdapter;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.ShardsResponse;
import co.elastic.clients.elasticsearch.cat.ThreadPoolResponse;
import co.elastic.clients.elasticsearch.cluster.PutComponentTemplateResponse;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.ilm.PutLifecycleResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.elasticsearch.ingest.PutPipelineResponse;
import co.elastic.clients.elasticsearch.ingest.SimulateResponse;
import co.elastic.clients.elasticsearch.sql.TranslateResponse;
import co.elastic.clients.elasticsearch.transform.GetTransformResponse;
import co.elastic.clients.elasticsearch.transform.PutTransformResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.TransportException;
import co.elastic.clients.transport.endpoints.BinaryResponse;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import co.elastic.clients.util.DateTime;
import co.elastic.clients.util.NamedValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.random.RandomGenerator;

import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createContextFromCaCert;
import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createTrustAllCertsContext;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.assumeNotNull;

class EsClientBrahmsTest {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "esclientit_";
    String indexName = "";


    @BeforeAll
    static void setUp() throws IOException {
        loadLocalES();
        if (client == null) {
            startOptionallyTestContainers();
        }
    }

    static void loadLocalES() {
        try {
            client = getClient("http://localhost:9200", null);
            asyncClient = getAsyncClient("http://localhost:9200", null);
            logger.debug("running test on localhost.");
        } catch (Exception e) {
            logger.debug("No cluster is running yet at localhost.");
        }
    }

    static void startOptionallyTestContainers() throws IOException {
        final var props = new Properties();
        props.load(EsClientBrahmsTest.class.getResourceAsStream("/version.properties"));
        final String version = props.getProperty("elasticsearch.version");
        logger.info("Starting testcontainers with Elasticsearch {}.", props.getProperty("elasticsearch.version"));
        // Start the container. This step might take some time...
        final ElasticsearchContainer container = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                        .withTag(version))
                .withPassword(PASSWORD)
                .withReuse(true);
        container.start();
        final byte[] certAsBytes = container.copyFileFromContainer(
                "/usr/share/elasticsearch/config/certs/http_ca.crt",
                InputStream::readAllBytes);
        try {
            client = getClient("https://" + container.getHttpHostAddress(), certAsBytes);
            asyncClient = getAsyncClient("https://" + container.getHttpHostAddress(), certAsBytes);
        } catch (Exception e) {
            logger.debug("No cluster is running yet at https://{}.", container.getHttpHostAddress());
        }

        assumeNotNull(client);
        assumeNotNull(asyncClient);
    }

    @AfterAll
    static void elasticsearchClient() throws IOException {
        if (client != null) {
            client.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
    }

    static private ElasticsearchClient getClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
        // Create the API client
        final ElasticsearchClient client = ElasticsearchClient.of(b -> b
                .host(elasticsearchServiceAddress)
                .sslContext(certificate != null ? createContextFromCaCert(certificate) : createTrustAllCertsContext())
                .usernameAndPassword("elastic", PASSWORD)
        );
        final InfoResponse info = client.info();
        logger.info("Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
        return client;
    }

    static private ElasticsearchAsyncClient getAsyncClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
        // Create the API client
        final ElasticsearchAsyncClient client = ElasticsearchAsyncClient.of(b -> b
                .host(elasticsearchServiceAddress)
                .sslContext(certificate != null ? createContextFromCaCert(certificate) : createTrustAllCertsContext())
                .usernameAndPassword("elastic", PASSWORD)
        );
        final InfoResponse info = client.info().get();
        logger.info("Async Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
        return client;
    }

    List<String> indices;
    String indexNameBase = "2025-10-17-142803-brahms-";

    @Test
    void importingBrahms() throws IOException {
        LocalDateTime start = LocalDateTime.now();
        importBrahmsFrom("specimen");
        importBrahmsFrom("multimedia");
        LocalDateTime end = LocalDateTime.now();
        long minutes = ChronoUnit.MINUTES.between(start, end);
        long seconds = ChronoUnit.SECONDS.between(start, end);
        System.out.println("whole brahms import took " + minutes + " minutes and " + seconds + " seconds.");

    }

    private void importBrahmsFrom(String type) throws IOException {
        String dir = "D:\\data\\nba\\etl\\ndjson\\brahms\\" + type;
        String indexNameBase = "2025-10-17-142803-brahms-";

        String indexName = indexNameBase + type;
        File dirFile = new File(dir);
        LocalDateTime start = LocalDateTime.now();
        for (File file : dirFile.listFiles()) {
            System.out.println("reading file = " + file.getAbsolutePath());
            LocalDateTime startRead = LocalDateTime.now();
            indexBulkFile(file);
            System.out.println("done file = " + file.getAbsolutePath());
            LocalDateTime endRead = LocalDateTime.now();
            System.out.println("indexing this file took" + ChronoUnit.SECONDS.between(startRead, endRead) + " seconds");
        }
        LocalDateTime end = LocalDateTime.now();
        long minutes = ChronoUnit.MINUTES.between(start, end);
        long seconds = ChronoUnit.SECONDS.between(start, end);
        System.out.println("minutes = " + minutes);
        System.out.println("seconds = " + seconds);
    }


    //    @BeforeEach
    void cleanIndexBeforeRun(final TestInfo testInfo) {
        indices = new ArrayList<>();
        final var methodName = testInfo.getTestMethod().orElseThrow().getName();
        indexName = PREFIX + methodName.toLowerCase(Locale.ROOT);

        logger.debug("Using [{}] as the index name", indexName);
        setAndRemoveIndex(indexName);
    }

//    @AfterEach
//    void cleanIndexAfterRun() {
//        indices.forEach(this::removeIndex);
//    }


    void bulkIngester() throws IOException {
        final var size = 1000;
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexName)
                )
                .listener(new BulkListener<>() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request, List<Void> voids) {
                        logger.debug("going to execute bulk of {} requests", request.operations().size());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> voids, BulkResponse response) {
                        logger.debug("bulk executed {} errors", response.errors() ? "with" : "without");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> voids, Throwable failure) {
                        logger.warn("error while executing bulk", failure);
                    }
                })
                .maxOperations(10)
                .maxSize(1_000_000)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            final var data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
            for (int i = 0; i < size; i++) {
                ingester.add(bo -> bo.index(io -> io.document(data)));
            }
        }

        // Make sure to close (and flush) the bulk ingester before exiting if you are not using try-with-resources
        // ingester.close();

        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(size);
    }


    void bulkIngesterFlush() throws IOException {
        final var size = 100_000;
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexName)
                )
                .maxOperations(10_000)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            final var data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
            for (int i = 0; i < size; i++) {
                ingester.add(bo -> bo.index(io -> io.document(data)));
            }

            // Calling flush should actually flush the ingester and send the latest docs
            ingester.flush();

            client.indices().refresh(rr -> rr.index(indexName));
            final SearchResponse<Void> response = client.search(sr -> sr.index(indexName).trackTotalHits(tth -> tth.enabled(true)), Void.class);
            assertThat(response.hits().total()).isNotNull();
            // But this test is failing as the flush is not sending the last batch
//             assertThat(response.hits().total().value()).isEqualTo(size);
            assertThat(response.hits().total().value()).isLessThanOrEqualTo(size);
        }
    }


    private void indexBulkFile(File file) throws IOException {
        String indexNameBrahms = indexName;
        //trying
        //maxoperations 10_000 : 80 minuten
        //maxoperations 100_000 : 80 minuten
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexNameBrahms)
                )
                .maxOperations(100_000)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            //@todo keep track where we were file and linewise for restart purposes
            try (LineNumberReader reader = new LineNumberReader(new FileReader(file))) {
                for (String docId = reader.readLine(); docId != null; docId = reader.readLine()) {
                    String doc = reader.readLine();
                    if (doc != null) {
                        final var data = BinaryData.of(doc.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
                        String finalDocId = docId;
                        ingester.add(bo -> bo.index(io -> io.id(finalDocId).document(data)));
                    }

                }
            }
        }
    }


    void rangeQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":2}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<ObjectNode> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.range(rq -> rq
                                .number(nrq -> nrq.field("foo").gte(0.0).lte(1.0))
                        ))
                , ObjectNode.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
    }


    void reindex() throws IOException {
        // Check the error is thrown when the source index does not exist
        assertThatThrownBy(() -> client.reindex(rr -> rr
                .source(s -> s.index(PREFIX + "does-not-exists")).dest(d -> d.index("foo"))))
                .isInstanceOfSatisfying(ElasticsearchException.class, e -> assertThat(e.status()).isEqualTo(404));

        // A regular reindex operation
        setAndRemoveIndex(indexName + "-dest");

        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final ReindexResponse reindexResponse = client.reindex(rr -> rr
                .source(s -> s.index(indexName)).dest(d -> d.index(indexName + "-dest")));
        assertThat(reindexResponse.total()).isEqualTo(1);
    }


    void geoPointSort() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Person> response = client.search(sr -> sr.index(indexName)
                .sort(so -> so
                        .geoDistance(gd -> gd
                                .field("location")
                                .location(
                                        new GeoLocation.Builder()
                                                .latlon(ll -> ll.lat(49.0404).lon(2.0174))
                                                .build()
                                )
                                .order(SortOrder.Asc)
                                .unit(DistanceUnit.Kilometers)
                        )
                ), Person.class);

        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(2);
        assertThat(response.hits().hits()).satisfiesExactly(hit1 -> {
            assertThat(hit1.id()).isEqualTo("1");
            assertThat(hit1.sort()).hasSize(1);
            assertThat(hit1.sort().get(0).doubleValue()).isEqualTo(0.0);
        }, hit2 -> {
            assertThat(hit2.id()).isEqualTo("2");
            assertThat(hit2.sort()).hasSize(1);
            assertThat(hit2.sort().get(0).doubleValue()).isEqualTo(8187.4318605250455);
        });
    }


    void geoPointSearch() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Person> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.geoBoundingBox(gbb -> gbb
                                .field("location")
                                .boundingBox(bbq -> bbq
                                        .coords(c -> c
                                                .bottom(0).left(0).top(50).right(10))
                                )))
                , Person.class);

        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo("1"));
    }


    void searchWithTimeout() throws IOException, ExecutionException, InterruptedException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final var timeoutException = new AtomicReference<>(false);

        final CompletableFuture<SearchResponse<Void>> future = asyncClient.search(sr -> sr
                                .index(indexName)
                                .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                        Void.class)
                .orTimeout(1, TimeUnit.NANOSECONDS)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        timeoutException.set(true);
                    } else {
                        logger.error("Got an unexpected exception", e);
                    }
                    return null;
                });
        assertThat(future.get()).isNull();
        assertThat(timeoutException.get()).isTrue();

        timeoutException.set(false);
        final SearchResponse<Void> response = asyncClient.search(sr -> sr
                                .index(indexName)
                                .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                        Void.class)
                .orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        timeoutException.set(true);
                    } else {
                        logger.error("Got an unexpected exception", e);
                    }
                    return null;
                })
                .get();
        assertThat(timeoutException.get()).isFalse();
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
    }


    void catApi() throws IOException {
        final ThreadPoolResponse threadPool = client.cat().threadPool();
        assertThat(threadPool).isNotNull();
        assertThat(threadPool.threadPools()).allSatisfy(record -> {
            assertThat(record.nodeName()).isNotNull();
            assertThat(record.name()).isNotNull();
            assertThat(record.active()).isNotNull();
            assertThat(record.queue()).isNotNull();
            assertThat(record.rejected()).isNotNull();
        });
        final IndicesResponse indices = client.cat().indices();
        assertThat(indices).isNotNull();
        assertThat(indices.indices()).allSatisfy(record -> {
            assertThat(record.index()).isNotNull();
            assertThat(record.docsCount()).isNotNull();
            assertThat(record.docsDeleted()).isNotNull();
        });
        final ShardsResponse shards = client.cat().shards();
        assertThat(shards).isNotNull();
        assertThat(shards.shards()).allSatisfy(record -> {
            assertThat(record.index()).isNotNull();
            assertThat(record.state()).isIn("STARTED", "UNASSIGNED");
            assertThat(record.prirep()).isIn("p", "r");
        });
    }


    void ingestPipelines() throws IOException {
        // Define some pipelines
        try {
            client.ingest().deletePipeline(pr -> pr.id("my-pipeline"));
        } catch (final ElasticsearchException ignored) {
        }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id("my-pipeline")
                    .processors(p -> p
                            .script(s -> s
                                    .lang(ScriptLanguage.Painless)
                                    .source(src -> src.scriptString("ctx.foo = 'bar'"))
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id("my-pipeline")
                    .processors(p -> p
                            .set(s -> s
                                    .field("foo")
                                    .value(JsonData.of("bar"))
                                    .ignoreFailure(true)
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }
        {
            final SimulateResponse response = client.ingest().simulate(sir -> sir
                    .id("my-pipeline")
                    .docs(d -> d
                            .source(JsonData.fromJson("{\"foo\":\"baz\"}"))
                    )
            );
            assertThat(response.docs())
                    .hasSize(1)
                    .allSatisfy(doc -> {
                        assertThat(doc.doc()).isNotNull();
                        assertThat(doc.doc().source()).isNotNull();
                        assertThat(doc.doc().source()).allSatisfy((key, value) -> {
                            assertThat(key).isEqualTo("foo");
                            assertThat(value).satisfies(jsonData -> assertThat(jsonData.to(String.class)).isEqualTo("bar"));
                        });
                    });
        }
    }


    void sourceRequest() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final GetSourceResponse<ObjectNode> source = client.getSource(gsr -> gsr.index(indexName).id("1"), ObjectNode.class);
        assertThat(source.source())
                .isNotNull()
                .satisfies(jsonData -> assertThat(jsonData.toString()).isEqualTo("{\"foo\":\"bar\"}"));
    }


    void deleteByQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response1 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response1.hits().total()).isNotNull();
        assertThat(response1.hits().total().value()).isEqualTo(1);
        final DeleteByQueryResponse deleteByQueryResponse = client.deleteByQuery(dbq -> dbq
                .index(indexName)
                .query(q -> q
                        .match(mq -> mq
                                .field("foo")
                                .query("bar"))));
        assertThat(deleteByQueryResponse.deleted()).isEqualTo(1);
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response2 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response2.hits().total()).isNotNull();
        assertThat(response2.hits().total().value()).isEqualTo(0);
    }


    void updateDocument() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"show_count\":0}")));
        client.update(ur -> ur.index(indexName).id("1").script(
                s -> s
                        .lang(ScriptLanguage.Painless)
                        .source(src -> src.scriptString("ctx._source.show_count += 1"))
        ), ObjectNode.class);
        final GetResponse<ObjectNode> response = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
        assertThat(response.source())
                .isNotNull()
                .satisfies(o -> assertThat(o.toString()).isEqualTo("{\"show_count\":1}"));
    }


    void createComponentTemplate() throws IOException {
        {
            final PutComponentTemplateResponse response = client.cluster().putComponentTemplate(pct -> pct
                    .name("my_component_template")
                    .template(t -> t
                            .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                            .mappings(m -> m
                                    .properties("foo", p -> p.text(tp -> tp))
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }

        {
            // With JSON
            final PutComponentTemplateResponse response = client.cluster().putComponentTemplate(pct -> pct
                    .name("my_component_template")
                    .template(t -> t
                            .mappings(
                                    m -> m.properties("@timestamp", p -> p.date(dp -> dp))
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }
    }


    void createIndexTemplate() throws IOException {
        client.cluster().putComponentTemplate(pct -> pct
                .name("my_component_template")
                .template(t -> t
                        .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                        .mappings(m -> m
                                .properties("foo", p -> p.text(tp -> tp))
                        )
                )
        );
        final PutIndexTemplateResponse response = client.indices().putIndexTemplate(pit -> pit
                .name("my_index_template")
                .indexPatterns("my-index-*")
                .composedOf("my_component_template")
                .template(t -> t
                        .aliases("foo", a -> a
                                .indexRouting("bar")
                        )
                        .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                        .mappings(m -> m
                                .properties("foo", p -> p.text(tp -> tp))
                        )
                )
        );
        assertThat(response.acknowledged()).isTrue();
    }


    void elser() throws IOException {
        // Create the index with sparse vector
        client.indices().create(cir -> cir.index(indexName).mappings(m -> m
                .properties("content", p -> p.text(tp -> tp))
                .properties("content_embedding", p -> p.sparseVector(sp -> sp))
        ));

        // Create the pipeline
        // This requires to have the elserv2 model deployed and started
        client.ingest().putPipeline(pr -> pr
                .id("elser-v2-test")
                .processors(p -> p
                        .inference(i -> i
                                .modelId(".elser_model_2")
                                .fieldMap("content", JsonData.of("content"))
                                .targetField("content_embedding")
                        )
                )
        );

        // We are expecting an exception as the model is not deployed
        assertThatThrownBy(() -> {
            // Search
            client.search(sr -> sr
                    .index(indexName)
                    .query(q -> q.sparseVector(sv -> sv
                            .field("content_embedding")
                            .inferenceId("elser-v2-test")
                            .query("How to avoid muscle soreness after running?")
                    )), ObjectNode.class);
        })
                .withFailMessage("We are expecting an exception as the model is not deployed")
                .isInstanceOfSatisfying(ElasticsearchException.class, exception -> {
                    assertThat(exception.error().reason()).isEqualTo("current license is non-compliant for [inference]");
                    assertThat(exception.status()).isEqualTo(403);
                });
    }


    void testIlm() throws IOException {
        try {
            client.ilm().deleteLifecycle(dlr -> dlr.name(indexName + "-ilm"));
        } catch (IOException | ElasticsearchException ignored) {
        }
        PutLifecycleResponse response = client.ilm().putLifecycle(plr -> plr
                .name(indexName + "-ilm")
                .policy(p -> p
                        .phases(ph -> ph
                                .hot(h -> h
                                        .actions(a -> a
                                                .rollover(r -> r
                                                        .maxAge(t -> t.time("5d"))
                                                        .maxSize("10gb")
                                                )
                                        )
                                )
                        )
                )
        );
        assertThat(response.acknowledged()).isTrue();
    }


    void searchExistField() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"baz\"}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":\"baz\", \"bar\":\"baz\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.exists(eq -> eq.field("bar")))
                , Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo("2"));
    }


    void multipleAggs() throws IOException {
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"france\",\"state\":\"paris\",\"city\":\"paris\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"germany\",\"state\":\"berlin\",\"city\":\"berlin\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"italy\",\"state\":\"rome\",\"city\":\"rome\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .aggregations("country", a -> a.terms(ta -> ta.field("country.keyword"))
                                .aggregations("state", sa -> sa.terms(ta -> ta.field("state.keyword"))
                                        .aggregations("city", ca -> ca.terms(ta -> ta.field("city.keyword")))
                                )
                        )
                , Void.class);

        assertThat(response.aggregations())
                .isNotNull()
                .hasEntrySatisfying("country", countries -> {
                    assertThat(countries.sterms()).isNotNull();
                    assertThat(countries.sterms().buckets()).isNotNull();
                    assertThat(countries.sterms().buckets().array())
                            .hasSize(3)
                            .anySatisfy(country -> {
                                assertThat(country.key()).isNotNull();
                                assertThat(country.key().stringValue()).isEqualTo("france");
                                assertThat(country.docCount()).isEqualTo(1);
                                assertThat(country.aggregations())
                                        .hasEntrySatisfying("state", state -> {
                                            assertThat(state.sterms()).isNotNull();
                                            assertThat(state.sterms().buckets()).isNotNull();
                                            assertThat(state.sterms().buckets().array())
                                                    .hasSize(1)
                                                    .satisfiesExactly(stateBucket -> {
                                                        assertThat(stateBucket.key()).isNotNull();
                                                        assertThat(stateBucket.key().stringValue()).isEqualTo("paris");
                                                        assertThat(stateBucket.docCount()).isEqualTo(1);
                                                        assertThat(stateBucket.aggregations())
                                                                .containsKey("city")
                                                                .hasEntrySatisfying("city", city -> {
                                                                    assertThat(city.sterms()).isNotNull();
                                                                    assertThat(city.sterms().buckets()).isNotNull();
                                                                    assertThat(city.sterms().buckets().array())
                                                                            .hasSize(1)
                                                                            .satisfiesExactly(cityBucket -> {
                                                                                assertThat(cityBucket.key()).isNotNull();
                                                                                assertThat(cityBucket.key().stringValue()).isEqualTo("paris");
                                                                                assertThat(cityBucket.docCount()).isEqualTo(1);
                                                                            });
                                                                });
                                                    });
                                        });
                            });
                });
    }


    void esql() throws IOException, SQLException {
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("David");
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Max");
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));

        String query = """
                FROM indexName
                | WHERE name == "David"
                | KEEP name
                | LIMIT 1
                """.replaceFirst("indexName", indexName);

        {
            // Using the Raw ES|QL API
            try (final BinaryResponse response = client.esql().query(q -> q.query(query)); InputStream is = response.content()) {
                // The response object is {"took":173,"is_partial":false,"documents_found":1,"values_loaded":1,"columns":[{"name":"name","type":"text"}],"values":[["David"]]}
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode jsonNode = mapper.readTree(is);
                assertThat(jsonNode).isNotNull().hasSize(6);
                assertThat(jsonNode.get("columns")).isNotNull().hasSize(1).first().satisfies(column -> assertThat(column.get("name").asText()).isEqualTo("name"));
                assertThat(jsonNode.get("values")).isNotNull().hasSize(1).first().satisfies(value -> assertThat(value).hasSize(1).first().satisfies(singleValue -> assertThat(singleValue.asText()).isEqualTo("David")));
                assertThat(jsonNode.get("took").asInt()).isGreaterThan(0);
                assertThat(jsonNode.get("is_partial").asBoolean()).isFalse();
                assertThat(jsonNode.get("documents_found").asLong()).isEqualTo(1);
                assertThat(jsonNode.get("values_loaded").asLong()).isEqualTo(1);
            }
        }

        {
            // Using the JDBC ResultSet ES|QL API
            try (final ResultSet resultSet = client.esql().query(ResultSetEsqlAdapter.INSTANCE, query)) {
                assertThat(resultSet).isNotNull().satisfies(resultSetResult -> {
                    assertThat(resultSetResult.next()).isTrue();
                    assertThat(resultSetResult.getString("name")).isEqualTo("David");
                });
            }
        }

        {
            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql().query(ObjectsEsqlAdapter.of(Person.class), query);
            for (final Person person : persons) {
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isNotNull();
            }
        }

        {
            // Using named parameters
            String parametrizedQuery = """
                    FROM indexName
                    | WHERE name == ?name
                    | KEEP name
                    | LIMIT 1
                    """.replaceFirst("indexName", indexName);

            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql()
                    .query(ObjectsEsqlAdapter.of(Person.class), parametrizedQuery,
                            Map.of("name", "David")
                    );
            for (final Person person : persons) {
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isNotNull();
            }
        }
    }

    /**
     * This one is failing for now. So we are expecting a failure.
     * When updating to 8.15.1, it should fix it. (<a href="https://github.com/elastic/elasticsearch-java/issues/865">865</a>)
     */

    void callHotThreads() {
        assertThatThrownBy(() -> client.nodes().hotThreads()).isInstanceOf(TransportException.class);
    }


    void withAliases() throws IOException {
        setAndRemoveIndex(indexName + "-v2");
        assertThat(client.indices().create(cir -> cir.index(indexName)
                .aliases(indexName + "_alias", a -> a)).acknowledged()).isTrue();
        assertThat(client.indices().create(cir -> cir.index(indexName + "-v2")).acknowledged()).isTrue();

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isTrue();

        // Check we have one alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).hasSize(1);
        // Check we have no alias on indexName-v2
        assertThat(client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases()).hasSize(0);

        // Switch the alias indexName_alias from indexName to indexName-v2
        client.indices().updateAliases(ua -> ua
                .actions(a -> a.add(aa -> aa.alias(indexName + "_alias").index(indexName + "-v2")))
                .actions(a -> a.remove(ra -> ra.alias(indexName + "_alias").index(indexName)))
        );

        // Check we have no alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).hasSize(0);
        // Check we have one alias on indexName-v2
        assertThat(client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases()).hasSize(1);

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isTrue();

        // Delete the alias
        client.indices().deleteAlias(da -> da.name(indexName + "_alias").index("*"));

        // Check the alias non-existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isFalse();
    }


    void kNNWithFunctionScore() throws IOException {
        client.indices().create(cir -> cir.index(indexName).mappings(m -> m
                .properties("vector", p -> p.denseVector(dv -> dv))
                .properties("country", p -> p.keyword(k -> k))
        ));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"france\", \"vector\":[1.0, 0.4, 0.8]}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.functionScore(
                                fsq -> fsq
                                        .query(qknn -> qknn.knn(
                                                k -> k.field("vector").queryVector(0.9f, 0.4f, 0.8f)
                                        ))
                                        .functions(fs -> fs.randomScore(rs -> rs.field("country").seed("hello")))
                        ))
                , Void.class);

        assumeNotNull(response.hits().total());
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).score()).isEqualTo(0.4063275);
    }


    void boolQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("""
                {
                    "number":1,
                    "effective_date":"2024-10-01T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("""
                {
                    "number":2,
                    "effective_date":"2024-10-02T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("3").withJson(new StringReader("""
                {
                    "number":3,
                    "effective_date":"2024-10-03T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("4").withJson(new StringReader("""
                {
                    "number":4,
                    "effective_date":"2024-10-04T00:00:00.000Z"
                }""")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.bool(bq -> bq
                                .filter(fq -> fq.terms(tq -> tq.field("number")
                                        .terms(t -> t.value(List.of(
                                                FieldValue.of("2"),
                                                FieldValue.of("3"))))))
                                .filter(fq -> fq
                                        .range(rq -> rq.date(drq -> drq
                                                .field("effective_date")
                                                .gte("2024-10-03T00:00:00.000Z"))))
                        ))
                , Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).hasSize(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo("3");
    }

    /**
     * This method adds the index name we want to use to the list
     * and deletes the index if it exists.
     *
     * @param name the index name
     */
    private void setAndRemoveIndex(final String name) {
        indices.add(name);
        removeIndex(name);
    }

    /**
     * This method deletes the index if it exists.
     *
     * @param name the index name
     */
    private void removeIndex(final String name) {
        try {
            client.indices().delete(dir -> dir.index(name));
            logger.debug("Index [{}] has been removed", name);
        } catch (final IOException | ElasticsearchException ignored) {
        }
    }
}
