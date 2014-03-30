package net.ravendb.tests.querying;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.document.DocumentQuery;
import net.ravendb.tests.querying.QIndexedUser;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;


public class UsingStronglyTypedDocumentQueryTest {
  private IDocumentQuery<IndexedUser> createUserQuery() {
    return new DocumentQuery<>(IndexedUser.class, null, null, "IndexName", null, null, null, false);
  }
  private Date mkDate(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  @Test
  public void whereEqualsSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereEquals("Name", "ayende", false).toString(),
        createUserQuery().whereEquals(x.name, "ayende", false).toString());
    assertEquals(createUserQuery().whereEquals("Name", "ayende").toString(),
        createUserQuery().whereEquals(x.name, "ayende").toString());
  }

  @Test
  public void whereInSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereIn("Name", Arrays.asList("ayende", "tobias")).toString(),
        createUserQuery().whereIn(x.name, Arrays.asList("ayende", "tobias")).toString());
  }

  @Test
  public void whereStartsWithSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereStartsWith("Name", "ayende").toString(),
        createUserQuery().whereStartsWith(x.name, "ayende").toString());
  }

  @Test
  public void whereEndsWithSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereEndsWith("Name", "ayende").toString(),
        createUserQuery().whereEndsWith(x.name, "ayende").toString());
  }

  @Test
  public void whereBetweenSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereBetween("Name", "ayende", "zaphod").toString(),
        createUserQuery().whereBetween(x.name, "ayende", "zaphod").toString());
  }

  @Test
  public void whereBetweenOrEqualSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereBetweenOrEqual("Name", "ayende", "zaphod").toString(),
        createUserQuery().whereBetweenOrEqual(x.name, "ayende", "zaphod").toString());
  }

  @Test
  public void whereGreaterThanSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereGreaterThan("Birthday", mkDate(1970, 1, 1)).toString(),
        createUserQuery().whereGreaterThan(x.birthday, mkDate(1970, 1, 1)).toString());
  }

  @Test
  public void whereGreaterThanOrEqualSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereGreaterThanOrEqual("Birthday", mkDate(1970, 1, 1)).toString(),
        createUserQuery().whereGreaterThanOrEqual(x.birthday, mkDate(1970, 1, 1)).toString());
  }

  @Test
  public void whereLessThanSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereLessThan("Birthday", mkDate(1970, 1, 1)).toString(),
        createUserQuery().whereLessThan(x.birthday, mkDate(1970, 1, 1)).toString());
  }

  @Test
  public void whereLessThanOrEqualSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().whereLessThanOrEqual("Birthday", mkDate(1970, 1, 1)).toString(),
        createUserQuery().whereLessThanOrEqual(x.birthday, mkDate(1970, 1, 1)).toString());
  }

  @Test
  public void searchSameAsUntypedCounterpart() {
    QIndexedUser x = QIndexedUser.indexedUser;
    assertEquals(createUserQuery().search("Name", "ayende").toString(),
        createUserQuery().search(x.name, "ayende").toString());
  }

  @Test
  public void canUseStronglyTypedAddOrder() {
    QIndexedUser x = QIndexedUser.indexedUser;
    createUserQuery().addOrder(x.birthday, false);
  }

  @Test
  public void canUseStronglyTypedOrderBy() {
    QIndexedUser x = QIndexedUser.indexedUser;
    createUserQuery().orderBy(x.birthday);
  }

  @Test
  public void canUseStronglyTypedSearch() {
    QIndexedUser x = QIndexedUser.indexedUser;
    createUserQuery().search(x.birthday, "1975");
  }

}
