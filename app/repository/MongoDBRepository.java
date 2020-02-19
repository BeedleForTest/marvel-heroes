package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.HeroSamples;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        // return HeroSamples.staticHero(heroId);
        // TODO
        String query = "{ \"id\" : \"" + heroId + "\" }";
        Document document = Document.parse(query);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        //return CompletableFuture.completedFuture(new ArrayList<>());

        List<String> instructions = new ArrayList<>();
        String sort = "{$sort: {\"_id.yearAppearance\": 1}}";
        instructions.add(sort);
        String match = "{$match: {\"identity.yearAppearance\": {\"$ne\": \"\"}}}";
        instructions.add(match);
        String group_year_universe = " { $group: {_id: { yearAppearance: \"$identity.yearAppearance\", universe: \"$identity.universe\"},count: {$sum: 1} } }";
        instructions.add(group_year_universe);
        String push = "{ $group: {_id: { yearAppearance: \"$_id.yearAppearance\"}, by_universe: {$push: {universe: \"$_id.universe\", count: \"$count\"}}}}";
        instructions.add(push);


        // TODO
        List<Document> pipeline = new ArrayList<>();
        instructions.forEach(i -> pipeline.add(Document.parse(i)));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("by_universe");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        // return CompletableFuture.completedFuture(new ArrayList<>());
        List<String> instructions = new ArrayList<>();

        String unwind = "{ $unwind: \"$powers\"}";
        instructions.add(unwind);
        String regroup_powers_count = "{ $group: { _id: \"$powers\", count: {$sum: 1}}}";
        instructions.add(regroup_powers_count);
        String sort_powers = "{ $sort:  {count: -1}}";
        instructions.add(sort_powers);
        String get_first = "{ $limit: "+ top + "}";
        instructions.add(get_first);

        // TODO
        List<Document> pipeline = new ArrayList<>();
        instructions.forEach(i -> pipeline.add(Document.parse(i)));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                            .map(Document::toJson)
                            .map(Json::parse)
                            .map(jsonNode -> {
                                return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                            })
                            .collect(Collectors.toList());
                });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        // return CompletableFuture.completedFuture(new ArrayList<>());
        String by_universe = "{ " +
                "$group: {" +
                    "_id: \"$identity.universe\"," +
                    "count: {" +
                        "$sum: 1" +
                    "}" +
                "}" +
                "}";
        // TODO
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(by_universe));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                            .map(Document::toJson)
                            .map(Json::parse)
                            .map(jsonNode -> {
                                return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                            })
                            .collect(Collectors.toList());
                });
    }

}
