/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.hms;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestS3ASchemeUpdater {
    private static final String S3_LOCATION = "s3://ehms-tables/test/";
    private static final String NON_S3_LOCATION = "sx://ehms-tables/test/";

    @Test
    public void testIsS3Scheme() {
        Table table = buildTestTable(S3_LOCATION);
        assertTrue(S3ASchemeUpdater.isTableUsingS3Scheme(table));
    }

    @Test
    public void testUpdateS3Scheme() {
        Table table = buildTestTable(S3_LOCATION);
        S3ASchemeUpdater.updateTableToUseS3AScheme(table);
        assertEquals("s3a://ehms-tables/test/", table.getSd().getLocation());
    }

    @Test
    public void testIsNotS3Scheme() {
        Table table = buildTestTable(NON_S3_LOCATION);
        assertFalse(S3ASchemeUpdater.isTableUsingS3Scheme(table));
    }

    @Test
    public void testIsNullLocation() {
        Table table = buildTestTable(null);
        assertFalse(S3ASchemeUpdater.isTableUsingS3Scheme(table));
    }

    private Table buildTestTable(String location) {
        Table table = new Table();
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(location);
        table.setSd(sd);
        return table;
    }
}
