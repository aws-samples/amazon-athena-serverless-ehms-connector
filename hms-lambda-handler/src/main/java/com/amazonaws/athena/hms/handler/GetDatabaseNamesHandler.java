/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.hms.handler;

import com.amazonaws.athena.hms.GetDatabaseNamesRequest;
import com.amazonaws.athena.hms.GetDatabaseNamesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Database;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class GetDatabaseNamesHandler extends BaseHMSHandler<GetDatabaseNamesRequest, GetDatabaseNamesResponse>
{
  public GetDatabaseNamesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  public Set<String> getDatabaseNames(String filter, HiveMetaStoreClient client) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllDatabases());
    }
    return client.getAllDatabases()
            .stream()
            .filter(n -> n.matches(filter))
            .collect(Collectors.toSet());
  }

  @Override
  public GetDatabaseNamesResponse handleRequest(GetDatabaseNamesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Fetching all database names with filter: " + request.getFilter());
      Set<String> databases = getDatabaseNames(request.getFilter(), client);
      context.getLogger().log("Fetched database names: " + (databases == null || databases.isEmpty() ? 0 : databases.size()));
      GetDatabaseNamesResponse response = new GetDatabaseNamesResponse();
      response.setDatabases(databases);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
