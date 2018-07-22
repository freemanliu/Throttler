package com.freemanliu;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Throttles give id according the configure rules, which is a json list of {@link ConfigItem} including
 * field id, intervalSeconds and tokensPerInternal. Each rule specify how many tokens the specified id
 * can consume in each interval. Each request consumes one token. If token count is equal or less than 0,
 * the request is rejected, otherwise allowed. Token count is refilled to tokenPerInterval each interval.
 * See {@link SimpleThrottlerTest} for example of configuration json.
 * <p>
 * Usage:
 * SimpleThrottler throttler = new SimpleThrottler(clock);
 * throttler.loadConfig(configFile);
 * throttler.start();
 * ...
 * throttler.allow(id);
 * ...
 * throttler.stop();
 * <p>
 * loadConfig can be called again, e.g., configure file change detected. After loading, start()
 * must be called, or all ids will be rejected.
 * This class is thread safe.
 */

public class SimpleThrottler implements Throttler {
    private final Logger logger = LoggerFactory.getLogger(SimpleThrottler.class);

    private final Clock clock;

    private class ConfigItem {
        private String id;
        private long intervalSeconds;
        private long tokensPerInterval;
        private long nextUpdateMilli;
        private long tokenCount;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(ConfigItem.class)
                    .add("id", id)
                    .add("intervalSeconds", intervalSeconds)
                    .add("tokensPerInterval", tokensPerInterval)
                    .add("nextUpdateMilli", getNextUpdateMilli())
                    .add("tokenCount", tokenCount)
                    .toString();
        }

        public long getNextUpdateMilli() {
            return nextUpdateMilli;
        }

        /**
         * Returns whether tokan count > 0, then decrement it.
         */
        public boolean consumeToken() {
            tokenCount--;
            return tokenCount >= 0;
        }

        public String getId() {
            return id;
        }

        public void refillToken() {
            tokenCount = tokensPerInterval;
            nextUpdateMilli += intervalSeconds * 1000;
        }

        public void initialize(long currentMilli) {
            nextUpdateMilli = currentMilli;
        }
    }

    private List<ConfigItem> configItemList = new ArrayList<>();
    private final Map<String, ConfigItem> idToItemMap = new HashMap<>();
    private boolean started;
    private Timer timer;

    public SimpleThrottler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public synchronized boolean allow(String id) {
        if (!started) {
            logger.error("Throttler not started yet. Reject request.");
            return false;
        }
        ConfigItem configItem = idToItemMap.getOrDefault(id, null);
        if (configItem == null) {
            logger.warn("Config for id {} not found. Reject request.", id);
            return false;
        }
        logger.debug("configItem: {}", configItem);
        return configItem.consumeToken();
    }

    public synchronized void loadConfig(String configFilePath) throws FileNotFoundException {
        Reader configReader = new FileReader(new File(configFilePath));
        loadConfig(configReader);
    }

    public synchronized void loadConfig(Reader configReader) {
        stop();

        Gson gson = new Gson();
        try {
            Type listType = new TypeToken<List<ConfigItem>>() {
            }.getType();
            configItemList = gson.fromJson(configReader, listType);
        } finally {
            try {
                configReader.close();
            } catch (IOException e) {
                logger.warn("Close reader error", e);
            }
        }

        idToItemMap.clear();
        for (ConfigItem configItem : configItemList) {
            logger.info("ConfigItem: {}", configItem);
            idToItemMap.put(configItem.getId(), configItem);
        }
    }

    public synchronized void start() {
        Preconditions.checkState(!configItemList.isEmpty(), "No config found.");
        Preconditions.checkState(!started);
        configItemList.forEach(item -> {
            item.initialize(clock.millis());
            item.refillToken();
        });
        timer = new Timer();
        scheduleRefilling();
        started = true;
    }

    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        timer.cancel();
        timer = null;
    }

    private void scheduleRefilling() {
        sortConfigItems();
        long delay = Math.max(0, configItemList.get(0).getNextUpdateMilli() - clock.millis());
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    logger.info("TimerTask");
                    refillTokens();
                    scheduleRefilling();
                } catch (Throwable t) {
                    logger.error("Refilling failed!", t);
                }
            }
        }, delay);
    }

    private void refillTokens() {
        long currentMillis = clock.millis();
        for (ConfigItem configItem : configItemList) {
            if (currentMillis > configItem.getNextUpdateMilli()) {
                configItem.refillToken();
            } else {
                break;
            }
        }
    }

    private void sortConfigItems() {
        Collections.sort(configItemList, new Comparator<ConfigItem>() {
            @Override
            public int compare(ConfigItem a, ConfigItem b) {
                return (int) (a.getNextUpdateMilli() - b.getNextUpdateMilli());
            }
        });
    }
}
