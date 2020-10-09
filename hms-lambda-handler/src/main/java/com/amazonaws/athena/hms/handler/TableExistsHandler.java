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
import com.amazonaws.athena.hms.TableExistsRequest;
import com.amazonaws.athena.hms.TableExistsResponse;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class TableExistsHandler extends BaseHMSHandler<TableExistsRequest, TableExistsResponse>
{
  public TableExistsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public TableExistsResponse handleRequest(TableExistsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to embedded HMS client");
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Checking if " + request.getDbName() + "." + request.getTableName() + " exists");
      boolean exists = client.tableExists(request.getDbName(), request.getTableName());
      context.getLogger().log("Table exists: " + exists);
      TableExistsResponse response = new TableExistsResponse();
      response.setExists(exists);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
