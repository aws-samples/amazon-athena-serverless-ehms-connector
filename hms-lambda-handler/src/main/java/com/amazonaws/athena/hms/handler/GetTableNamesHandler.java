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

import com.amazonaws.athena.hms.GetTableNamesRequest;
import com.amazonaws.athena.hms.GetTableNamesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class GetTableNamesHandler extends BaseHMSHandler<GetTableNamesRequest, GetTableNamesResponse>
{
  public GetTableNamesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  public Set<String> getTableNames(String dbName, String filter, HiveMetaStoreClient client) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllTables(dbName));
    }
    return client.getAllTables(dbName)
            .stream()
            .filter(n -> n.matches(filter))
            .collect(Collectors.toSet());
  }

  @Override
  public GetTableNamesResponse handleRequest(GetTableNamesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Fetching all table names for DB: " + request.getDbName() + " with filter: " + request.getFilter());
      Set<String> tables = getTableNames(request.getDbName(), request.getFilter(), client);
      context.getLogger().log("Fetched table names: " + (tables == null || tables.isEmpty() ? 0 : tables.size()));
      GetTableNamesResponse response = new GetTableNamesResponse();
      response.setTables(tables);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
