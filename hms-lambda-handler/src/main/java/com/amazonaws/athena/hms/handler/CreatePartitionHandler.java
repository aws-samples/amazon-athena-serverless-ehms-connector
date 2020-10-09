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

import com.amazonaws.athena.hms.CreatePartitionRequest;
import com.amazonaws.athena.hms.CreatePartitionResponse;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import java.nio.charset.StandardCharsets;

public class CreatePartitionHandler extends BaseHMSHandler<CreatePartitionRequest, CreatePartitionResponse>
{
  static class PartitionBuilder
  {
    private final Table table;
    private List<String> values;
    private String location;
    private Map<String, String> parameters = new HashMap<>();

    private PartitionBuilder()
    {
      table = null;
    }

    PartitionBuilder(Table table)
    {
      this.table = table;
    }

    PartitionBuilder withValues(List<String> values)
    {
      this.values = new ArrayList<>(values);
      return this;
    }

    PartitionBuilder withLocation(String location)
    {
      this.location = location;
      return this;
    }

    PartitionBuilder withParameter(String name, String value)
    {
      parameters.put(name, value);
      return this;
    }

    PartitionBuilder withParameters(Map<String, String> params)
    {
      parameters = params;
      return this;
    }

    Partition build()
    {
      Partition partition = new Partition();
      List<String> partitionNames = table.getPartitionKeys()
              .stream()
              .map(FieldSchema::getName)
              .collect(Collectors.toList());
      if (partitionNames.size() != values.size()) {
        throw new RuntimeException("Partition values do not match table schema");
      }
      List<String> spec = IntStream.range(0, values.size())
              .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
              .collect(Collectors.toList());

      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      partition.setParameters(parameters);
      partition.setValues(values);
      partition.setSd(table.getSd().deepCopy());
      if (this.location == null) {
        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
      }
      else {
        partition.getSd().setLocation(location);
      }
      return partition;
    }
  }

  public CreatePartitionHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public CreatePartitionResponse handleRequest(CreatePartitionRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Creating table with desc: " + request.getTableDesc());
      TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
      Table table = new Table();
      deserializer.fromString(table, request.getTableDesc());
      Partition partition = client.add_partition(new PartitionBuilder(table).withValues(request.getValues()).build());
      context.getLogger().log("Created partition: " + partition);
      CreatePartitionResponse response = new CreatePartitionResponse();
      if (partition != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        response.setPartitionDesc(serializer.toString(partition, StandardCharsets.UTF_8.name()));
      }
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
