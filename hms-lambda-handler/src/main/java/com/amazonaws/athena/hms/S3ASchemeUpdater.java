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

import org.apache.hadoop.hive.metastore.api.Table;

public class S3ASchemeUpdater {
    private static final String S3_SCHEME = "s3:";
    private static final String S3A_SCHEME = "s3a:";

    public static void updateTableToUseS3AScheme(Table table) {
        String location = table.getSd().getLocation();
        location = S3A_SCHEME + location.substring(3);
        table.getSd().setLocation(location);
    }
    public static boolean isTableUsingS3Scheme(Table table) {
        String location = table.getSd().getLocation();
        return location != null && location.length() >= 3 && S3_SCHEME.equals(location.substring(0, 3));
    }
}
