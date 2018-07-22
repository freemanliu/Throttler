package com.freemanliu;

import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.time.Clock;
import java.time.ZoneId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleThrottlerTest {

    private static final String ID1 = "ID1";
    private static final String ID2 = "ID2";
    private static final String ID3 = "ID3";
    private SimpleThrottler simpleThrottler;

    @Before
    public void setup() {
        String configString = "[" +
                "{" +
                "\"id\": \"ID1\"," +
                "\"intervalSeconds\": 5," +
                "\"tokensPerInterval\": 10" +
                "}," +                "{" +
                "\"id\": \"ID2\"," +
                "\"intervalSeconds\": 10," +
                "\"tokensPerInterval\": 100" +
                "}" +
                "]";
        simpleThrottler = new SimpleThrottler(Clock.system(ZoneId.systemDefault()));
        simpleThrottler.loadConfig(new StringReader(configString));
    }

    @Test
    public void testAllowNotStart() {
        assertFalse(simpleThrottler.allow(ID1));
        assertFalse(simpleThrottler.allow(ID2));
        assertFalse(simpleThrottler.allow(ID3));
    }

    @Test
    public void testAllowStarted() {
        simpleThrottler.start();
        assertTrue(simpleThrottler.allow(ID1));
        assertTrue(simpleThrottler.allow(ID2));
        assertFalse(simpleThrottler.allow(ID3));
        simpleThrottler.stop();
    }

    @Test
    public void testAllowThrottled() {
        simpleThrottler.start();
        assertJustNRequest(10, ID1);
        assertJustNRequest(100, ID2);
        try {
            // Give one more second to make sure the timer is tiggered.
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            fail("Who interrupt my sleep?!");
        }
        assertJustNRequest(10, ID1);
        assertJustNRequest(0, ID2);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            fail("Who interrupt my sleep?!");
        }
        assertJustNRequest(10, ID1);
        assertJustNRequest(100, ID2);
        simpleThrottler.stop();
    }

    private void assertJustNRequest(int n, String id) {
        for (int i = 0; i < n; i++) {
            assertTrue(simpleThrottler.allow(id));
        }
        assertFalse(simpleThrottler.allow(id));
    }
}