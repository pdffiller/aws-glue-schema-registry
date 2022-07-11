package com.amazonaws.services.schemaregistry.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;

@Slf4j
public class DataPlatformSchemaNamingStrategy implements AWSSchemaNamingStrategy {

    /**
     * Returns the schemaName.
     *
     * @param transportName topic Name or stream name etc.
     * @param data
     * @param isKey
     * @return schema name.
     */
    @Override
    public String getSchemaName(String transportName, Object data, boolean isKey) {
        if (data instanceof GenericData.Record) {
            return transportName + (isKey ? "-Key" : "-Value");
        }
        return getSchemaName(transportName);
    }


    /**
     * Returns the schemaName.
     *
     * @param transportName topic name
     * @return schema name.
     */
    @Override
    public String getSchemaName(String transportName) {
        return transportName;
    }
}

