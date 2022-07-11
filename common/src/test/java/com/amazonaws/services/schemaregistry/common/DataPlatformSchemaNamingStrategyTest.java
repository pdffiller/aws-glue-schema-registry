package com.amazonaws.services.schemaregistry.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class DataPlatformSchemaNamingStrategyTest {
    @Test
    public void test_dataPlatformSchemaNamingStrategy() throws IOException {
        String fileName = "src/test/java/resources/avro/user3.avsc";

        Schema schema = new Schema.Parser().parse(new File(fileName));

        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schema, "ONE");
        GenericData.Record genericRecord = new GenericData.Record(schema);


        String topicName = "User-Topic";

        String keySchemaName = new DataPlatformSchemaNamingStrategy()
                .getSchemaName(topicName, genericRecord, true);
        assertEquals(topicName + "-Key", keySchemaName);

        String valueSchemaName = new DataPlatformSchemaNamingStrategy()
                .getSchemaName(topicName, genericRecord, false);
        assertEquals(topicName + "-Value", valueSchemaName);

        String schemaNameAsTopic = new DataPlatformSchemaNamingStrategy()
                .getSchemaName(topicName, enumSymbol, false);
        assertEquals(topicName, schemaNameAsTopic);
    }
}
