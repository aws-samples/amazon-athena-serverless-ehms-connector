/*-
 * #%L
 * hms-lambda-rnp
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena;

import com.amazonaws.athena.conf.Configuration;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;

public class MockHiveMetaStoreConf extends HiveMetaStoreConf
{
  public static final String HMS_RESPONSE_RECORD_LOCATION = "hive.metastore.response.record.location";
  public static final String ENV_RECORD_LOCATION = "RECORD_LOCATION";

  // the root s3 path to store the recorded response file
  private String responseRecordLocation;

  public String getResponseRecordLocation()
  {
    return responseRecordLocation;
  }

  public void setResponseRecordLocation(String responseRecordLocation)
  {
    this.responseRecordLocation = responseRecordLocation;
  }

  public static MockHiveMetaStoreConf loadAndOverrideExtraEnvironmentVariables()
  {
    Configuration hmsConf = Configuration.loadDefaultFromClasspath(HMS_PROPERTIES);

    MockHiveMetaStoreConf conf = new MockHiveMetaStoreConf();

    conf.setConnectionURL(hmsConf.getProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname));
    conf.setConnectionDriverName(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER.varname));
    conf.setConnectionPassword(hmsConf.getProperty(HiveConf.ConfVars.METASTOREPWD.varname));
    conf.setConnectionUserName(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.varname));
    conf.setMetastoreWarehouse(hmsConf.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));

    conf.setMetastoreSetUgi(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, true));
    conf.setResponseSpillLocation(hmsConf.getProperty(HMS_RESPONSE_SPILL_LOCATION));
    conf.setResponseSpillThreshold(hmsConf.getLong(HMS_RESPONSE_SPILL_THRESHOLD, DEFAULT_HMS_RESPONSE_SPILL_THRESHOLD));
    conf.setHandlerNamePrefix(hmsConf.getString(HMS_HANDLER_NAME_PREFIX, DEFAULT_HMS_HANDLER_NAME_PREFIX));
    conf.setResponseSpillLocation(hmsConf.getProperty(HMS_RESPONSE_RECORD_LOCATION));
    // override parameters with Lambda Environment variables
    // String hmsUris = System.getenv(ENV_HMS_URIS);

    String spillLocation = System.getenv(ENV_SPILL_LOCATION);
    if (!Strings.isNullOrEmpty(spillLocation)) {
      conf.setResponseSpillLocation(spillLocation);
    }
    String recordLocation = System.getenv(ENV_RECORD_LOCATION);
    if (!Strings.isNullOrEmpty(recordLocation)) {
      conf.setResponseRecordLocation(recordLocation);
    }
    return conf;
  }

  @Override
  public String toString()
  {
    return "{" +
            "connectionURL: " + getConnectionURL() + '\'' +
            ", connectionDriverName: " + getConnectionDriverName() + '\'' +
            ", connectionUserName: " + getConnectionUserName() + '\'' +
            ", connectionPassword: '" + getConnectionPassword() + '\'' +
            ", metaWarehouse: '" + getMetaWarehouse() + '\'' +
            ", metastoreSetUgi: " + isMetastoreSetUgi() +
            ", metastoreUri: '" + getMetastoreUri() + '\'' +
            ", responseSpillThreshold: " + getResponseSpillThreshold() +
            ", responseSpillLocation: '" + getResponseSpillLocation() + '\'' +
            ", responseRecordLocation: '" + getResponseRecordLocation() + '\'' +
            ", handlerNamePrefix: '" + getHandlerNamePrefix() + '\'' +
            '}';
  }
}
