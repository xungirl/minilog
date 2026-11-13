package dev.minilog.group;

import dev.minilog.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ToIntFunction;

/**
 * Range assignment, as in Kafka's RangeAssignor: for each topic, sort the subscribed members and
 * give each a contiguous range of partitions. With 7 partitions and 3 members:
 * {@code m1 -> [0,1,2], m2 -> [3,4], m3 -> [5,6]} (the first {@code 7 % 3} members get one extra).
 */
public final class RangeAssignor {
    private RangeAssignor() {
    }

    /**
     * @param subscriptions  memberId -> subscribed topics
     * @param partitionCount topic -> number of partitions
     */
    public static Map<String, List<TopicPartition>> assign(Map<String, Set<String>> subscriptions,
                                                           ToIntFunction<String> partitionCount) {
        Map<String, List<TopicPartition>> result = new HashMap<>();
        subscriptions.keySet().forEach(m -> result.put(m, new ArrayList<>()));
        Set<String> allTopics = new TreeSet<>();
        subscriptions.values().forEach(allTopics::addAll);
        for (String topic : allTopics) {
            List<String> members = subscriptions.entrySet().stream()
                    .filter(e -> e.getValue().contains(topic))
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            int partitions = partitionCount.applyAsInt(topic);
            int perMember = partitions / members.size();
            int extra = partitions % members.size();
            int next = 0;
            for (int i = 0; i < members.size(); i++) {
                int count = perMember + (i < extra ? 1 : 0);
                for (int j = 0; j < count; j++) {
                    result.get(members.get(i)).add(new TopicPartition(topic, next++));
                }
            }
        }
        return result;
    }
}
