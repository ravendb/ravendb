package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentQuery;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.client.linq.RavenQueryInspector;
import net.ravendb.tests.linq.QWhereClauseTest_Dog;
import net.ravendb.tests.linq.QWhereClauseTest_IndexedUser;
import net.ravendb.tests.linq.QWhereClauseTest_Renamed;
import net.ravendb.tests.linq.QWhereClauseTest_UserProperty;

import org.apache.commons.lang.time.DateUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.After;
import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class WhereClauseTest {

  private IDocumentStore store;
  private IDocumentSession session;


  public WhereClauseTest() {
    store = new DocumentStore("http://fake");
    store.initialize();
    session = store.openSession();
  }

  @QueryEntity
  public static class Renamed {
    @JsonProperty("Yellow")
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @Test
  public void willRespectRenames() throws JsonGenerationException, JsonMappingException, IOException {
    QWhereClauseTest_Renamed x = QWhereClauseTest_Renamed.renamed;
    String q = session.query(Renamed.class).where(x.name.eq("red")).toString();
    Renamed r = new Renamed();
    r.setName("N");
    String valueAsString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(r);
    assertEquals("{\"Yellow\":\"N\"}", valueAsString);
    assertEquals("Yellow:red", q);
  }


  @Test
  public void handlesNegative() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser x = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(x.active.not());
    assertEquals("Active:false", q.toString());
  }

  @Test
  public void handlesNegativeEquality() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser x = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(x.active.eq(false));
    assertEquals("Active:false", q.toString());
  }

  @Test
  public void handleDoubleRangeSearch() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser x = QWhereClauseTest_IndexedUser.indexedUser;
    double min = 1246.434565380224, max = 1246.434565380226;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(x.rate.goe(min).and(x.rate.loe(max)));
    assertEquals("Rate_Range:[Dx1246.43456538022 TO Dx1246.43456538023]", q.toString());
  }

  @Test
  public void canHandleCasts() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser x = QWhereClauseTest_IndexedUser.indexedUser;

    IRavenQueryable<IndexedUser> q = indexedUsers.where(x.animal.as(QWhereClauseTest_Dog.class).color.eq("Black"));
    assertEquals("Animal.Color:Black", q.toString());
  }

  @Test
  public void startsWith() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.startsWith("foo"));
    assertEquals("Name:foo*", q.toString());
  }

  @Test
  public void startsWithEqTrue() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.startsWith("foo").eq(true));
    assertEquals("Name:foo*", q.toString());
  }

  @Test
  public void startsWithEqFalse() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.startsWith("foo").eq(false));
    assertEquals("(*:* AND -Name:foo*)", q.toString());
  }

  @Test
  public void startsWithNegated() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.startsWith("foo").not());
    assertEquals("(*:* AND -Name:foo*)", q.toString());
  }

  @Test
  public void isNullOrEmpty() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isEmpty());
    assertEquals("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.toString());
  }

  @Test
  public void isNullOrEmptyEqTrue() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isEmpty().eq(true));
    assertEquals("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.toString());
  }

  @Test
  public void isNullOrEmptyEqFalse() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isEmpty().eq(false));
    assertEquals("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.toString());
  }

  @Test
  public void isNullOrEmptyNegated() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isEmpty().not());
    assertEquals("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.toString());
  }

  @Test
  public void isNullOrEmpty_Any() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isNotEmpty());
    assertEquals("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.toString());
  }

  @Test
  public void isNullOrEmpty_AnyEqTrue() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isNotEmpty().eq(true));
    assertEquals("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.toString());
  }

  @Test
  public void isNullOrEmpty_AnyEqFalse() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isNotEmpty().eq(false));
    assertEquals("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.toString());
  }

  @Test
  public void isNullOrEmpty_AnyNegated() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.isNotEmpty().eq(false));
    assertEquals("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.toString());
  }

  @Test
  public void bracesOverrideOperatorPrecedence_second_method() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;

    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende").and(user.name.eq("rob").or(user.name.eq("dave"))));
    assertEquals("Name:ayende AND (Name:rob OR Name:dave)", q.toString());
  }

  @Test
  public void bracesOverrideOperatorPrecedence_third_method() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende"));
    q = q.where(user.name.eq("rob").or(user.name.eq("dave")));
    assertEquals("(Name:ayende) AND (Name:rob OR Name:dave)", q.toString());
  }

  /**
   *
   */
  @Test
  public void canForceUsingIgnoreCase() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.equalsIgnoreCase("ayende"));
    assertEquals("Name:ayende", q.toString());
  }

  @Test
  public void canForceUsingCase() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.equalsNotIgnoreCase("ayende"));
    assertEquals("Name:[[ayende]]", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyGT() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.lt(15));
    assertEquals("Age_Range:{* TO Ix15}", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyGE() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.loe(15));
    assertEquals("Age_Range:[* TO Ix15]", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyLT() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.gt(15));
    assertEquals("Age_Range:{Ix15 TO NULL}", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyLE() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.goe(15));
    assertEquals("Age_Range:[Ix15 TO NULL]", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyEQ() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.eq(15));
    assertEquals("Age:15", q.toString());
  }

  @Test
  //we can't use value then property in QueryDSL
  public void canCompareValueThenPropertyNE() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.ne(15));
    assertEquals("(-Age:15 AND Age:*)", q.toString());
  }

  @Test
  public void canUnderstandSimpleEquality() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende"));
    assertEquals("Name:ayende", q.toString());
  }

  private RavenQueryInspector<IndexedUser> getRavenQueryInspector() {
    return (RavenQueryInspector<IndexedUser>) session.query(IndexedUser.class);
  }

  private RavenQueryInspector<IndexedUser> getRavenQueryInspectorStatic() {
    return (RavenQueryInspector<IndexedUser>) session.query(IndexedUser.class, "static");
  }

  @Test
  public void canUnderstandSimpleEqualityWithVariable() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    String ayende = "ayende" + 1;
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq(ayende));
    assertEquals("Name:ayende1", q.toString());
  }

  @Test
  public void canUnderstandSimpleContains() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende"));
    assertEquals("Name:ayende", q.toString());
  }

  @Test
  public void canUnderstandSimpleStartsWithInExpression1() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.startsWith("ayende"));
    assertNotNull(q);
    assertEquals("Name:ayende*", q.toString());
  }

  @Test
  public void canUnderstandSimpleContainsWithVariable() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    String ayende = "ayende" + 1;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq(ayende));

    assertEquals("Name:ayende1", q.toString());
  }

  @Test
  public void noOpShouldProduceEmptyString() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    IRavenQueryable<IndexedUser> q = indexedUsers;
    assertEquals("", q.toString());
  }

  @Test
  public void canUnderstandAnd() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende").and(user.email.eq("ayende@ayende.com")));
    assertEquals("Name:ayende AND Email:ayende@ayende.com", q.toString());
  }

  @Test
  public void canUnderstandOr() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende").or(user.email.eq("ayende@ayende.com")));
    assertEquals("Name:ayende OR Email:ayende@ayende.com", q.toString());
  }

  @Test
  public void withNoBracesOperatorPrecedenceIsHonored() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where((user.name.eq("ayende").and(user.name.eq("rob")).or(user.name.eq("dave"))));
    assertEquals("(Name:ayende AND Name:rob) OR Name:dave", q.toString());
  }

  @Test
  public void bracesOverrideOperatorPrecedence() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("ayende").and(user.name.eq("rob").or(user.name.eq("dave"))));
    assertEquals("Name:ayende AND (Name:rob OR Name:dave)", q.toString());
  }

  @Test
  public void canUnderstandLessThan() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.lt(mkDate(2010, 5, 15)));
    assertEquals("Birthday:{* TO 2010-05-15T00:00:00.0000000Z}", q.toString());

  }

  private Date mkDate(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  @Test
  public void negatingSubClauses() {
    IDocumentQuery<Object> query = new DocumentQuery<>(Object.class, null, null, null, null, null, null, false).not()
      .openSubclause()
      .whereEquals("IsPublished", true)
      .andAlso()
      .whereEquals("Tags.Length", 0)
      .closeSubclause();

    assertEquals("-(IsPublished:true AND Tags.Length:0)", query.toString());
  }

  @Test
  public void canUnderstandEqualOnDate() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.eq(mkDate(2010, 5, 15)));
    assertEquals("Birthday:2010-05-15T00:00:00.0000000Z", q.toString());
  }

  @Test
  public void canUnderstandLessThanOrEqualsTo() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.loe(mkDate(2010, 5, 15)));
    assertEquals("Birthday:[* TO 2010-05-15T00:00:00.0000000Z]", q.toString());
  }

  @Test
  public void canUnderstandGreaterThan() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.gt(mkDate(2010, 5, 15)));
    assertEquals("Birthday:{2010-05-15T00:00:00.0000000Z TO NULL}", q.toString());
  }

  @Test
  public void canUnderstandGreaterThanOrEqualsTo() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.goe(mkDate(2010, 5, 15)));
    assertEquals("Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", q.toString());
  }

  @Test
  public void canUnderstandProjectionOfOneField() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<String> q = indexedUsers.where(user.birthday.goe(mkDate(2010, 5, 15))).select(user.name);
    assertEquals("<Name>: Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", q.toString());
  }

  @Test
  public void canUnderstandProjectionOfMultipleFields() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.goe(mkDate(2010, 5, 15))).select(IndexedUser.class, user.name, user.age);
    assertEquals("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", q.toString());
  }

  @Test
  public void canUnderstandSimpleEqualityOnInt() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.eq(3));
    assertEquals("Age:3", q.toString());
  }

  @Test
  public void canUnderstandGreaterThanOnInt() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.gt(3));
    assertEquals("Age_Range:{Ix3 TO NULL}", q.toString());
  }

  @Test
  public void canUnderstandMethodCalls() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.birthday.goe(mkDate(2010, 5, 15))).select(IndexedUser.class, user.name, user.age);
    assertEquals("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000Z TO NULL]", q.toString());
  }

  @Test
  public void canUnderstandConvertExpressions() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.eq(3));
    assertEquals("Age:3", q.toString());
  }

  @Test
  public void canChainMultipleWhereClauses() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.age.eq(3)).where(user.name.eq("ayende"));
    assertEquals("(Age:3) AND (Name:ayende)", q.toString());
  }

  @Test
  public void canUnderstandSimpleAny_Dynamic() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    QWhereClauseTest_UserProperty p = QWhereClauseTest_UserProperty.userProperty;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.properties.any(p.key.eq("first")));
    assertEquals("Properties,Key:first", q.toString());
  }

  @Test
  public void canUnderstandSimpleAny_Static() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspectorStatic();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    QWhereClauseTest_UserProperty p = QWhereClauseTest_UserProperty.userProperty;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.properties.any(p.key.eq("first")));
    assertEquals("Properties_Key:first", q.toString());
  }

  @Test
  public void anyOnCollection() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.properties.isNotEmpty());
    assertEquals("Properties:*", q.toString());
  }

  @Test
  public void anyOnCollectionEqTrue() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.properties.isNotEmpty().eq(true));
    assertEquals("Properties:*", q.toString());
  }

  @Test
  public void anyOnCollectionEqFalse() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.properties.isNotEmpty().eq(false));
    assertEquals("(*:* AND -Properties:*)", q.toString());
  }

  @Test
  public void willWrapLuceneSaveKeyword_NOT() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("NOT"));
    assertEquals("Name:\"NOT\"", q.toString());
  }

  @Test
  public void willWrapLuceneSaveKeyword_OR() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("OR"));
    assertEquals("Name:\"OR\"", q.toString());
  }

  @Test
  public void willWrapLuceneSaveKeyword_AND() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("AND"));
    assertEquals("Name:\"AND\"", q.toString());
  }

  @Test
  public void willNotWrapCaseNotMatchedLuceneSaveKeyword_And() {
    RavenQueryInspector<IndexedUser> indexedUsers = getRavenQueryInspector();
    QWhereClauseTest_IndexedUser user = QWhereClauseTest_IndexedUser.indexedUser;
    IRavenQueryable<IndexedUser> q = indexedUsers.where(user.name.eq("And"));
    assertEquals("Name:And", q.toString());
  }

  @QueryEntity
  public static class IndexedUser {
    private int age;
    private Date birthday;
    private String name;
    private String email;
    private List<UserProperty> properties;
    private boolean active;
    private IAnimal animal;
    private double rate;
    public int getAge() {
      return age;
    }
    public void setAge(int age) {
      this.age = age;
    }
    public Date getBirthday() {
      return birthday;
    }
    public void setBirthday(Date birthday) {
      this.birthday = birthday;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getEmail() {
      return email;
    }
    public void setEmail(String email) {
      this.email = email;
    }
    public List<UserProperty> getProperties() {
      return properties;
    }
    public void setProperties(List<UserProperty> properties) {
      this.properties = properties;
    }
    public boolean isActive() {
      return active;
    }
    public void setActive(boolean isActive) {
      this.active = isActive;
    }
    public IAnimal getAnimal() {
      return animal;
    }
    public void setAnimal(IAnimal animal) {
      this.animal = animal;
    }
    public double getRate() {
      return rate;
    }
    public void setRate(double rate) {
      this.rate = rate;
    }

  }

  @QueryEntity
  public static interface IAnimal {

  }

  @QueryEntity
  public static class Dog implements IAnimal {
    private String color;

    public String getColor() {
      return color;
    }

    public void setColor(String color) {
      this.color = color;
    }

  }

  @QueryEntity
  public static class UserProperty {
    private String key;
    private String value;
    public String getKey() {
      return key;
    }
    public void setKey(String key) {
      this.key = key;
    }
    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }
  }

  @After
  public void cleanup() throws Exception {
    session.close();
    store.close();
  }



}
