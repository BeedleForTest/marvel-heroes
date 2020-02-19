package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        // TODO
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\n" +
                        "  \"query\": {\n" +
                        "    \"multi_match\" : {\n" +
                        "      \"query\":    \""+input+"\", \n" +
                        "      \"fields\": [ \"name\", \"aliases\", \"secretIdentities\", \"description\", \"partners\" ] \n" +
                        "    }\n" +
                        "  }\n" +
                        "}"))
                .thenApply(response -> {
                    JsonNode response_json = response.asJson();
                    int nbhits = response_json.get("hits").get("total").get("value").asInt();
                    int nbpages = nbhits/size + 1;
                    ArrayNode results = (ArrayNode) response_json.get("hits").get("hits");
                    List<SearchedHero> l2 = new ArrayList<>();
                    for (int i = 0; i < results.size(); i++) {
                        JsonNode node = results.get(i);
                        JsonNode node_source = node.get("_source");
                        l2.add(SearchedHero.fromJson(node_source));
                    }
                    return new PaginatedResults<>(nbhits, page, nbpages, l2);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        // return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search?pretty")
                .post(Json.parse("{\n" +
                        "    \"suggest\": {\n" +
                        "        \"hero-suggest\" : {\n" +
                        "            \"prefix\" : \"" + input + "\", \n" +
                        "            \"completion\" : { \n" +
                        "                \"field\" : \"suggest\" \n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}"))
                .thenApply(response -> {
                    JsonNode response_json = response.asJson();
                    ArrayNode suggests = (ArrayNode) response_json.get("suggest").get("hero-suggest");
                    ArrayNode options = (ArrayNode) suggests.get(0).get("options");
                    List<SearchedHero> l2 = new ArrayList<>();
                    for (int i = 0; i < options.size(); i++) {
                        JsonNode node = options.get(i);
                        JsonNode node_source = node.get("_source");
                        l2.add(SearchedHero.fromJson(node_source));
                    }
                    return l2;
                });
    }
}
