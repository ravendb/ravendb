package raven.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import raven.abstractions.data.DocumentChangeTypes;
import raven.abstractions.data.FacetAggregation;
import raven.abstractions.data.FacetAggregationSet;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.indexing.SortOptions;
import raven.client.document.FailoverBehavior;
import raven.client.document.FailoverBehaviorSet;


public class SerializationTest {
  @Test
  public void canSerializeUsingValues() throws JsonGenerationException, JsonMappingException, IOException {
    ObjectMapper objectMapper = JsonExtensions.createDefaultJsonSerializer();

    assertEquals("\"None\"", objectMapper.writeValueAsString(SortOptions.NONE));
    assertEquals("\"Custom\"", objectMapper.writeValueAsString(SortOptions.CUSTOM));
    assertEquals("\"BulkInsertEnded\"", objectMapper.writeValueAsString(DocumentChangeTypes.BULK_INSERT_ENDED));

    assertEquals("\"FailImmediately\"", objectMapper.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY)));
    assertEquals("\"AllowReadsFromSecondaries\"", objectMapper.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES)));
    assertEquals("\"AllowReadsFromSecondariesAndWritesToSecondaries\"", objectMapper.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES)));
    assertEquals("\"AllowReadsFromSecondariesAndWritesToSecondaries, ReadFromAllServers\"", objectMapper.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS)));
    assertEquals("\"None\"", objectMapper.writeValueAsString(new FacetAggregationSet(FacetAggregation.NONE)));

    assertEquals("\"AllowAllWildcards\"", objectMapper.writeValueAsString(EscapeQueryOptions.ALLOW_ALL_WILDCARDS));

    assertEquals(SortOptions.NONE, objectMapper.readValue("0", SortOptions.class));
    assertEquals(SortOptions.CUSTOM, objectMapper.readValue("9", SortOptions.class));
    assertEquals(DocumentChangeTypes.BULK_INSERT_ENDED, objectMapper.readValue("8", DocumentChangeTypes.class));

    assertEquals(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY), objectMapper.readValue("0", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES), objectMapper.readValue("1", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES), objectMapper.readValue("3", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS), objectMapper.readValue("1027", FailoverBehaviorSet.class));
    assertEquals(new FacetAggregationSet(FacetAggregation.NONE), objectMapper.readValue("0", FacetAggregationSet.class));


  }



}
