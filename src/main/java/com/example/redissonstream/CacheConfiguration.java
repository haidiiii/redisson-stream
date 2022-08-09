package com.example.redissonstream;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * @author fht
 * @since 2022-08-08 20:51
 */
@Configuration
public class CacheConfiguration {

    @Autowired
    private Environment env;

    @Bean
    public CacheManager cacheManager(HazelcastInstance hazelcastInstance) {
        return new com.hazelcast.spring.cache.HazelcastCacheManager(hazelcastInstance);
    }

    @Bean
    public HazelcastInstance hazelcastInstance() {
        HazelcastInstance hazelCastInstance = Hazelcast.getHazelcastInstanceByName("test");
        if (hazelCastInstance != null) {
            return hazelCastInstance;
        }
        Config config = new Config();
        config.setInstanceName("test");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.setManagementCenterConfig(new ManagementCenterConfig());

        config.getNetworkConfig().setPort(5701);
        config.getNetworkConfig().setPublicAddress("localhost:5701");
        config.getMapConfigs().put("default", initializeDefaultMapConfig());

        // Full reference is available at: https://docs.hazelcast.org/docs/management-center/3.9/manual/html/Deploying_and_Starting.html
//        config.setManagementCenterConfig(initializeDefaultManagementCenterConfig(jHipsterProperties));
        config.getMapConfigs().put("com.test.*", initializeDomainMapConfig());
        try {
            return Hazelcast.newHazelcastInstance(config);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private MapConfig initializeDefaultMapConfig() {
        MapConfig mapConfig = new MapConfig();

        /*
        Number of backups. If 1 is set as the backup-count for example,
        then all entries of the map will be copied to another JVM for
        fail-safety. Valid numbers are 0 (no backup), 1, 2, 3.
        */
        mapConfig.setBackupCount(1);

        /*
        Valid values are:
        NONE (no eviction),
        LRU (Least Recently Used),
        LFU (Least Frequently Used).
        NONE is the default.
        */
//        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.USED_HEAP_SIZE);
        mapConfig.setEvictionConfig(evictionConfig);
        /*
        Maximum size of the map. When max size is reached,
        map is evicted based on the policy defined.
        Any integer between 0 and Integer.MAX_VALUE. 0 means
        Integer.MAX_VALUE. Default is 0.
        */
//        mapConfig.setMaxSizeConfig(new MaxSizeConfig(0, MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE));

        return mapConfig;
    }

    private MapConfig initializeDomainMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(3600);
        return mapConfig;
    }

}
