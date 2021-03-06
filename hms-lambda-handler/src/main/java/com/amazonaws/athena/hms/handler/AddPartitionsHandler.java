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

import com.amazonaws.athena.hms.AddPartitionsRequest;
import com.amazonaws.athena.hms.AddPartitionsResponse;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TDeserializer;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.util.ArrayList;
import java.util.List;

public class AddPartitionsHandler extends BaseHMSHandler<AddPartitionsRequest, AddPartitionsResponse>
{
  public AddPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public AddPartitionsResponse handleRequest(AddPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      boolean isEmpty = request.getPartitionDescs() == null || request.getPartitionDescs().isEmpty();
      context.getLogger().log("Adding partitions: " +
          (isEmpty ? 0 : request.getPartitionDescs().size()));
      if (!isEmpty) {
        TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
        List<Partition> partitionList = new ArrayList<>();
        for (String partitionDesc : request.getPartitionDescs()) {
          Partition partition = new Partition();
          deserializer.fromString(partition, partitionDesc);
          partitionList.add(partition);
        }
        client.add_partitions(partitionList);
        context.getLogger().log("Added partitions: " + partitionList.size());
      }
      return new AddPartitionsResponse();
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
