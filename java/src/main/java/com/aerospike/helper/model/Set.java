/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.helper.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * this class represents a Set defined in the cluster
 *
 * @author peter
 */
public class Set {
    private Namespace parent;
    private String name;
    protected Map<String, NameValuePair> values;

    public Set(Namespace parent, String info) {
        this.parent = parent;
        setInfo(info);
    }

    public Object getParent() {
        return this.parent;
    }

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public boolean equals(Object obj) {
        return ((obj instanceof Set) &&
                (obj.toString().equals(toString())));
    }

    public String getName() {
        return toString();
    }

    public void setInfo(String info) {
        //ns_name=test:set_name=demo:n_objects=1:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false
        //ns_name=test:set_name=bad:name:n_objects=1:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false
        if (!info.isEmpty()) {
            String[] parts = splitWithFix(info);
            if (values == null) {
                values = new HashMap<>();
            }

            for (String part : parts) {
                String[] kv = part.split("=");
                String key = kv[0];

                // lenght should always be 2 - fix should prevent errors
                if (kv.length == 2) {
                    String value = kv[1];
                    NameValuePair storedValue = values.get(key);
                    if (storedValue == null) {
                        storedValue = new NameValuePair(this, key, value);
                        values.put(key, storedValue);
                    } else {
                        storedValue.value = value;
                    }
                }
            }
            applySetName();
        }
    }

    public void mergeSetInfo(String info) {
	//ns=test:set=selector:objects=1000:memory_data_bytes=0:deleting=false:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false
        if (!info.isEmpty()) {
            String[] parts = splitWithFix(info);
            if (values == null) {
                values = new HashMap<>();
            }

            for (String part : parts) {
                String[] kv = part.split("=");
                String key = kv[0];

                // lenght should always be 2 - fix should prevent errors
                if (kv.length == 2) {
                    String value = kv[1];
                    NameValuePair storedValue = values.get(key);
                    if (storedValue == null) {
                        storedValue = new NameValuePair(this, key, value);
                        values.put(key, storedValue);
                    } else {
                        try {
                            Long newValue = Long.parseLong(value);
                            Long oldValue = Long.parseLong(storedValue.value.toString());
                            storedValue.value = Long.toString(oldValue + newValue);
                        } catch (NumberFormatException e) {
                            storedValue.value = value;
                        }
                    }
                }
            }
            applySetName();
        }
    }

    public void setValues(Map<String, NameValuePair> newValues) {
        this.values = newValues;
    }

    public List<NameValuePair> getValues() {
        List<NameValuePair> result = new ArrayList<NameValuePair>();
        java.util.Set<String> keys = this.values.keySet();
        for (String key : keys) {
            NameValuePair nvp = this.values.get(key);
            result.add(nvp);
        }
        return result;
    }

    public void clear() {
        java.util.Set<String> keys = this.values.keySet();
        for (String key : keys) {
            NameValuePair nvp = this.values.get(key);
            nvp.clear();
        }
    }

    private void applySetName(){
        this.name = values.get("set") == null ? (String)values.get("set_name").value : (String)values.get("set").value;
    }

    /**
     * Fix to allow having sets that include ":" characters. <p>
     *
     * </p>Related to aerospike bug that allows creating sets with colons. If aerospike contains AT LEAST ONE set like that,
     * using this library without the fix would make throw an IndexOutOfBoundsException.
     */
    private String[] splitWithFix(final String sets) {
        final LinkedList<String> keyValuesSplitFix = new LinkedList<>();

        try {
            if (sets.length() > 3) {

                String key;
                String value = "";

                int keyEndIndex = sets.length();
                int valueEndIndex = sets.length();

                boolean valueIsSet = false;

                for (int i = sets.length() - 1; i >= 0; i--) {

                    if (!valueIsSet) {

                        if ('=' == sets.charAt(i)) {
                            value = sets.substring(i, valueEndIndex); // include equals too
                            keyEndIndex = i;
                            valueIsSet = true;
                        }

                    } else {

                        if (':' == sets.charAt(i)) {
                            key = sets.substring(i + 1, keyEndIndex);
                            keyValuesSplitFix.addFirst(key + value);
                            valueEndIndex = i;
                            valueIsSet = false;
                        }

                    }

                }

                key = sets.substring(0, keyEndIndex);
                keyValuesSplitFix.addFirst(key + value);

                return keyValuesSplitFix.toArray(new String[]{});
            }
        } catch (Exception e) {
            // do nothing
        }

        return new String[] {};

    }

}
