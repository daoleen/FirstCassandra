package com.daoleen.cassandra.intro;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alex on 1/27/15.
 */
public class CassandraIntroduction {
    private Cluster cluster;
    private Session session;

    public static void main(String[] args) {
        CassandraIntroduction ci = new CassandraIntroduction();
        ci.insertUser();
        ci.displayUsers();
    }

    public void displayUsers() {
        connect();

        ResultSet resultSet = session.execute("SELECT * FROM users");
        for(Row row : resultSet) {
            System.out.println(
                    String.format("twitter_id: %s, followers: %s",
                            row.getString("twitter_id"), row.getMap("followers", String.class, String.class))
            );
        }

        cluster.close();
    }


    public void insertUser() {
        connect();

        Map followers = new HashMap(2);
        followers.put("created_at", "2012-07-12");
        followers.put("follower", "alexssource");

        PreparedStatement statement = session.prepare("INSERT INTO users(twitter_id, followers) values (?,?)");
        BoundStatement bs = new BoundStatement(statement);
        bs.setString("twitter_id", "misha");
        bs.setMap("followers", followers);
        session.execute(bs.bind());

        cluster.close();
    }


    private final void connect() {
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy())
                ).build();
        session = cluster.connect("twitter");
    }
}
