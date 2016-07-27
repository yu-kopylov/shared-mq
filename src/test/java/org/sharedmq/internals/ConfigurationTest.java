package org.sharedmq.internals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(CommonTests.class)
public class ConfigurationTest {

    @Test
    public void testEquality() {
        Configuration config1 = new Configuration(100, 200);
        Configuration config2 = new Configuration(100, 200);
        Configuration config3 = new Configuration(101, 200);
        Configuration config4 = new Configuration(100, 201);

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1, config3);
        assertNotEquals(config1, config4);
    }
}
