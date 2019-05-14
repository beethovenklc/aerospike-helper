package com.aerospike.helper.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SetTest {

    @Test
    public void testSetInfo() {

        String sets = "a=b:c=d:set_name=f:g=h";

        Set underTest = new Set(null, sets);

        assertEquals("f", underTest.getName());
        assertEquals(4, underTest.values.size());

        assertEquals("b", underTest.values.get("a").value);
        assertEquals("f", underTest.values.get("set_name").value);
        assertEquals("d", underTest.values.get("c").value);
        assertEquals("h", underTest.values.get("g").value);

    }

    @Test
    public void testSetInfoWhenSetNameContainsColons() {

        String sets = "a=b:c=d:set_name=f:bugged:g=h";

        Set underTest = new Set(null, sets);

        assertEquals("f", underTest.getName());
        assertEquals(4, underTest.values.size());

        assertEquals("b", underTest.values.get("a").value);
        assertEquals("f", underTest.values.get("set_name").value);
        assertEquals("d", underTest.values.get("c").value);
        assertEquals("h", underTest.values.get("g").value);

    }

}