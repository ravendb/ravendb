package net.ravendb.tests.querying;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.document.DocumentQuery;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;


public class UsingDocumentQueryTest {

  @Test
  public void canUnderstandSimpleEquality() throws Exception {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Name", "ayende", false);
    assertEquals("Name:[[ayende]]", q.toString());
  }

  @Test
  public void canUnderstandSimpleEqualityWithVariable() {
    String ayende = "ayende" + 1;
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Name", ayende, false);
    assertEquals("Name:[[ayende1]]", q.toString());
  }

  @Test
  public void canUnderstandSimpleContains() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList("ayende"));
    assertEquals("@in<Name>:(ayende)", q.toString());
  }

  @Test
  public void canUnderstandParamArrayContains() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList(new String[] { "ryan", "heath"}));
    assertEquals("@in<Name>:(ryan,heath)", q.toString());
  }

  @Test
  public void canUnderstandArrayContains() {
    String[] array = new String[] { "ryan", "heath"} ;
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList(array));
    assertEquals("@in<Name>:(ryan,heath)", q.toString());
  }

  @Test
  public void canUnderstandArrayContainsWithPhrase() {
    String[] array = new String[] { "ryan", "heath here" };
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList(array));
    assertEquals("@in<Name>:(ryan,\"heath here\")", q.toString());
  }

  @Test
  public void canUnderstandArrayContainsWithOneElement() {
    String[] array = new String[] { "ryan" };
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList(array));
    assertEquals("@in<Name>:(ryan)", q.toString());
  }

  @Test
  public void canUnderstandArrayContainsWithZeroElements() {
    String[] array = new String[] {  };
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereIn("Name", Arrays.asList(array));
    assertEquals("@emptyIn<Name>:(no-results)", q.toString());
  }

  @Test
  public void noOpShouldProduceEmptyString() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false);
    assertEquals("", q.toString());
  }

  @Test
  public void canUnderstandAnd() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Name", "ayende")
        .andAlso()
        .whereEquals("Email", "ayende@ayende.com");
    assertEquals("Name:ayende AND Email:ayende@ayende.com", q.toString());
  }

  @Test
  public void canUnderstandOr() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Name", "ayende")
        .orElse()
        .whereEquals("Email", "ayende@ayende.com");
    assertEquals("Name:ayende OR Email:ayende@ayende.com", q.toString());
  }


  private Date mkDate(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  @Test
  public void canUnderstandLessThan() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereLessThan("Birthday", mkDate(2010, 5, 15));
    assertEquals("Birthday:{* TO 2010-05-15T00:00:00.0000000Z}", q.toString());
  }

  @Test
  public void canUnderstandEqualOnDate() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Birthday", mkDate(2010, 5, 15));
    assertEquals("Birthday:2010-05-15T00:00:00.0000000Z", q.toString());
  }

  @Test
  public void canUnderstandLessThanOrEqual() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereLessThanOrEqual("Birthday", mkDate(2010, 5, 15));
    assertEquals("Birthday:[* TO 2010-05-15T00:00:00.0000000Z]", q.toString());
  }

  @Test
  public void canUnderstandGreaterThan() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereGreaterThan("Birthday", mkDate(2010, 5, 15));
    assertEquals("Birthday:{2010-05-15T00:00:00.0000000Z TO NULL}", q.toString());
  }

  @Test
  public void canUnderstandGreaterThanOrEqual() {
    IDocumentQuery<IndexedUser> q = new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereGreaterThanOrEqual("Birthday", mkDate(2010, 5, 15));
    assertEquals("Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", q.toString());
  }

  @Test
  public void canUnderstandProjectionOfSingleField() {
    DocumentQuery<IndexedUser> q = (DocumentQuery<IndexedUser>) new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereGreaterThanOrEqual("Birthday", mkDate(2010, 5, 15))
        .selectFields(IndexedUser.class, "Name");
    String fields = q.getProjectionFields().isEmpty() ? "" : ("<" + StringUtils.join(q.getProjectionFields(), ", ") + ">: ");
    assertEquals("<Name>: Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", fields + q.toString());
  }

  @Test
  public void canUnderstandProjectionOfMultipleFields() {
    DocumentQuery<IndexedUser> q = (DocumentQuery<IndexedUser>) new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereGreaterThanOrEqual("Birthday", mkDate(2010, 5, 15))
        .selectFields(IndexedUser.class, "Name", "Age");
    String fields = q.getProjectionFields().isEmpty() ? "" : ("<" + StringUtils.join(q.getProjectionFields(), ", ") + ">: ");
    assertEquals("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", fields + q.toString());
  }

  @Test
  public void canUnderstandSimpleEqualityOnInt() {
    DocumentQuery<IndexedUser> q = (DocumentQuery<IndexedUser>) new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereEquals("Age", 3, false);
    assertEquals("Age:3", q.toString());
  }

  @Test
  public void canUnderstandGreaterThanOnInt() {
    // should DocumentQuery<T> understand how to generate range field names?
    DocumentQuery<IndexedUser> q = (DocumentQuery<IndexedUser>) new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false)
        .whereGreaterThan("Age_Range", 3);
    assertEquals("Age_Range:{Ix3 TO NULL}", q.toString());
  }

}
