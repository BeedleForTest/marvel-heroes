package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        System.out.println("Connected to Redis");
        RedisCommands<String, String> syncCommands = connection.sync();

        syncCommands.set(statItem.slug+":name", statItem.name);
        syncCommands.set(statItem.slug+":url", statItem.imageUrl);
        syncCommands.set(statItem.slug+":type", statItem.type);

        syncCommands.zincrby("heroes_visited", 1 ,statItem.slug);

        connection.close();
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        // TODO
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        System.out.println("Connected to Redis");
        RedisCommands<String, String> syncCommands = connection.sync();

        syncCommands.incr(statItem.slug+":count");

        connection.close();
        return CompletableFuture.completedFuture(true);
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        // TODO
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        System.out.println("Connected to Redis");
        RedisCommands<String, String> syncCommands = connection.sync();

        syncCommands.lpush("last_visited", statItem.slug);

        connection.close();
        return CompletableFuture.completedFuture(1L);
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        // TODO
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        System.out.println("Connected to Redis");
        RedisCommands<String, String> syncCommands = connection.sync();

        int nb = syncCommands.lrange("last_visited", 0, -1).size();
        List<String> l;
        if (nb > count) {
            l = syncCommands.lrange("last_visited", 0, count-1);
        } else {
            l = syncCommands.lrange("last_visited", 0, -1);
        }
        List<StatItem> l2 = new ArrayList<>();
        for (String slug : l) {
            String name = syncCommands.get(slug+":name");
            String url = syncCommands.get(slug+":url");
            String type = syncCommands.get(slug+":type");
            l2.add(new StatItem(slug, name, url, type));
        }

        connection.close();
        return CompletableFuture.completedFuture(l2);
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        // TODO
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        System.out.println("Connected to Redis");
        RedisCommands<String, String> syncCommands = connection.sync();

        int nb = syncCommands.zrange("heroes_visited", 0, -1).size();
        List<String> l;
        if (nb > count) {
            l = syncCommands.zrange("heroes_visited", 0-count, -1);
        } else {
            l = syncCommands.zrange("heroes_visited", 0, -1);
        }
        List<TopStatItem> l2 = new ArrayList<>();
        for (String slug : l) {
            String name = syncCommands.get(slug + ":name");
            String url = syncCommands.get(slug + ":url");
            String type = syncCommands.get(slug + ":type");
            long score = (long) syncCommands.zscore("heroes_visited", slug).intValue();
            l2.add(new TopStatItem(new StatItem(slug, name, url, type), score));
        }

        connection.close();
        Collections.reverse(l2);

        return CompletableFuture.completedFuture(l2);
    }
}
