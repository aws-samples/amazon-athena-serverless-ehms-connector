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

import com.amazonaws.athena.hms.DropPartitionsRequest;
import com.amazonaws.athena.hms.DropPartitionsResponse;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.thrift.TSerializer;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import java.util.List;

public class DropPartitionsHandler extends BaseHMSHandler<DropPartitionsRequest, DropPartitionsResponse>
{
  public DropPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  public List<String> getPartitionNames(String dbName, String tableName, short maxSize, HiveMetaStoreClient client) throws TException
  {
    return client.listPartitionNames(dbName, tableName, maxSize);
  }

  public boolean dropPartitions(String dbName, String tableName,
                                             List<String> partNames, HiveMetaStoreClient client) throws TException
  {
    boolean deleteData = false;

    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName, (short) -1, client), client);
    }
    if (partNames.isEmpty()) {
      return true;
    }

    return client.dropPartition(dbName, tableName, partNames, deleteData);
  }

  @Override
  public DropPartitionsResponse handleRequest(DropPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Dropping partitions for DB " + request.getDbName() + " table " + request.getTableName());
      boolean successful = dropPartitions(request.getDbName(), request.getTableName(), request.getPartNames(), client);
      context.getLogger().log("Dropped partitions for table " + request.getTableName() + " in DB " + request.getDbName());
      DropPartitionsResponse response = new DropPartitionsResponse();
      response.setSuccessful(successful);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
