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

import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.athena.hms.ListPartitionsRequest;
import com.amazonaws.athena.hms.ListPartitionsResponse;
import com.amazonaws.athena.hms.PaginatedResponse;
import com.amazonaws.athena.hms.Paginator;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class ListPartitionsHandler extends BaseHMSHandler<ListPartitionsRequest, ListPartitionsResponse>
{
  public ListPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  private static class PartitionPaginator extends Paginator<Partition>
  {
    private final Context context;
    private final ListPartitionsRequest request;
    private final HiveMetaStoreClient client;

    private PartitionPaginator(Context context, ListPartitionsRequest request, HiveMetaStoreClient client)
    {
      this.context = context;
      this.request = request;
      this.client = client;
    }

    @Override
    protected Collection<String> getNames() throws TException
    {
      context.getLogger().log("Fetching all partition names for DB: " + request.getDbName()
          + " table: " + request.getTableName());
      return client.listPartitionNames(request.getDbName(), request.getTableName(), (short) -1);
    }

    @Override
    protected List<Partition> getEntriesByNames(List<String> names) throws TException
    {
      if (names == null) {
        return client.getPartitionsByNames(request.getDbName(), request.getTableName(),
                client.listPartitionNames(request.getDbName(), request.getTableName(), (short) -1));
      }
      return client.getPartitionsByNames(request.getDbName(), request.getTableName(), names);
    }
  }

  @Override
  public ListPartitionsResponse handleRequest(ListPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      ListPartitionsResponse response = new ListPartitionsResponse();
      PartitionPaginator paginator = new PartitionPaginator(context, request, client);
      PaginatedResponse<Partition> paginatedResponse = paginator.paginateByNames(request.getNextToken(), request.getMaxSize());
      if (paginatedResponse != null) {
        response.setNextToken(paginatedResponse.getNextToken());
        List<Partition> partitions = paginatedResponse.getEntries();
        if (partitions != null && !partitions.isEmpty()) {
          TSerializer serializer = new TSerializer(getTProtocolFactory());
          List<String> jsonPartitionList = new ArrayList<>();
          for (Partition partition : partitions) {
            jsonPartitionList.add(serializer.toString(partition, StandardCharsets.UTF_8.name()));
          }
          response.setPartitions(jsonPartitionList);
          context.getLogger().log("Paginated response: entry size: " + jsonPartitionList.size()
              + ", nextToken: " + response.getNextToken());
        }
      }
      return response;
    }
    catch (RuntimeException e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw e;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
