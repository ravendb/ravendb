package net.ravendb.abstractions.data;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;


public class SharpEnumTest {
  @Test
  public void testEnumReadWrite() throws Exception {
    ObjectMapper mapper = JsonExtensions.createDefaultJsonSerializer();

    assertEquals("\"Short\"", mapper.writeValueAsString(SortOptions.SHORT));
    assertEquals(SortOptions.SHORT, mapper.readValue("\"Short\"", SortOptions.class));

    testDeserialization(mapper);

  }


  @Test
  public void testSaveEnumsAsIntegers() throws Exception {
    DocumentConvention convention = new DocumentConvention();
    convention.setSaveEnumsAsIntegers(true);
    ObjectMapper serializer = convention.createSerializer();

    assertEquals("0", serializer.writeValueAsString(SortOptions.NONE));
    assertEquals("7", serializer.writeValueAsString(SortOptions.DOUBLE));
    assertEquals("0", serializer.writeValueAsString(FieldIndexing.NO));
    assertEquals("2", serializer.writeValueAsString(FieldIndexing.NOT_ANALYZED));
    assertEquals("0", serializer.writeValueAsString(new FailoverBehaviorSet()));
    assertEquals("0", serializer.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY)));
    assertEquals("1027", serializer.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS)));

    testDeserialization(serializer);

  }

  private void testDeserialization(ObjectMapper mapper) throws Exception {
    assertEquals(SortOptions.NONE, mapper.readValue("0", SortOptions.class));
    assertEquals(SortOptions.NONE, mapper.readValue("\"None\"", SortOptions.class));

    assertEquals(SortOptions.DOUBLE, mapper.readValue("7", SortOptions.class));
    assertEquals(SortOptions.DOUBLE, mapper.readValue("\"Double\"", SortOptions.class));

    assertEquals(FieldIndexing.NO, mapper.readValue("\"No\"", FieldIndexing.class));
    assertEquals(FieldIndexing.NO, mapper.readValue("0", FieldIndexing.class));

    assertEquals(FieldIndexing.NOT_ANALYZED, mapper.readValue("\"NotAnalyzed\"", FieldIndexing.class));
    assertEquals(FieldIndexing.NOT_ANALYZED, mapper.readValue("2", FieldIndexing.class));

    assertEquals(new FailoverBehaviorSet(), mapper.readValue("0", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(), mapper.readValue("\"\"", FailoverBehaviorSet.class));

    assertEquals(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY), mapper.readValue("0", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY), mapper.readValue("\"\"", FailoverBehaviorSet.class));

    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS), mapper.readValue("1027", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS), mapper.readValue("\"AllowReadsFromSecondaries, AllowReadsFromSecondariesAndWritesToSecondaries, ReadFromAllServers\"", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS), mapper.readValue("\"AllowReadsFromSecondariesAndWritesToSecondaries, ReadFromAllServers\"", FailoverBehaviorSet.class));
    assertEquals(new FailoverBehaviorSet(FailoverBehavior.READ_FROM_ALL_SERVERS), mapper.readValue("\"ReadFromAllServers\"", FailoverBehaviorSet.class));

  }

  @Test
  public void testSaveEnumsAsStrings() throws Exception  {
    DocumentConvention convention = new DocumentConvention();
    convention.setSaveEnumsAsIntegers(false);
    ObjectMapper serializer = convention.createSerializer();

    assertEquals("\"None\"", serializer.writeValueAsString(SortOptions.NONE));
    assertEquals("\"Double\"", serializer.writeValueAsString(SortOptions.DOUBLE));
    assertEquals("\"No\"", serializer.writeValueAsString(FieldIndexing.NO));
    assertEquals("\"NotAnalyzed\"", serializer.writeValueAsString(FieldIndexing.NOT_ANALYZED));
    assertEquals("\"FailImmediately\"", serializer.writeValueAsString(new FailoverBehaviorSet()));
    assertEquals("\"FailImmediately\"", serializer.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY)));
    assertEquals("\"AllowReadsFromSecondariesAndWritesToSecondaries, ReadFromAllServers\"", serializer.writeValueAsString(new FailoverBehaviorSet(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES, FailoverBehavior.READ_FROM_ALL_SERVERS)));

    testDeserialization(serializer);

  }

}
