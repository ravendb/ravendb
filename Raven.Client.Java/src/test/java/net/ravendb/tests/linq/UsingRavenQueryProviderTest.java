package net.ravendb.tests.linq;

import static com.mysema.query.collections.CollQueryFactory.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.linq.QUser;
import net.ravendb.tests.linq.QUsingRavenQueryProviderTest_DateTimeInfo;
import net.ravendb.tests.linq.QUsingRavenQueryProviderTest_OrderItem;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class UsingRavenQueryProviderTest extends RemoteClientTest {

  @Test
  public void can_perform_Skip_Take_Query() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";

      try (IDocumentSession session = store.openSession()) {
        addData(session);

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Age} ");
        indexDefinitionBuilder.getSortOptions().put(x.name, SortOptions.STRING_VAL);

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();
        waitForNonStaleIndexes(store.getDatabaseCommands());

        IRavenQueryable<User> allResults = session.query(User.class, indexName).orderBy(x.name.asc()).where(x.age.gt(0));
        assertEquals(4, allResults.toList().size());

        IRavenQueryable<User> takeResults = session.query(User.class, indexName).orderBy(x.name.asc()).where(x.age.gt(0)).take(3);
        assertEquals(3, takeResults.toList().size());

        IRavenQueryable<User> skipResults = session.query(User.class, indexName).orderBy(x.name.asc()).where(x.age.gt(10)).skip(1);
        assertEquals(3, skipResults.toList().size());
        assertFalse(skipResults.toList().contains(firstUser));

        IRavenQueryable<User> skipTakeResults = session.query(User.class, indexName).orderBy(x.name.asc()).where(x.age.gt(10)).skip(1).take(2);
        assertEquals(2, skipTakeResults.toList().size());
        assertFalse(skipTakeResults.toList().contains(firstUser));
        assertFalse(skipTakeResults.toList().contains(lastUser));
      }
    }
  }

  @Test
  public void can_perform_First_and_FirstOrDefault_Query() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";
      try (IDocumentSession session = store.openSession()) {
        addData(session);

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Age} ");
        indexDefinitionBuilder.getSortOptions().put(x.name, SortOptions.STRING_VAL);

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();

        User firstItem = session.query(User.class, indexName)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .orderBy(x.name.asc()).first();
        assertEquals(firstUser, firstItem);

        //This should pull out the 1st parson ages 60, i.e. "Bob"
        User firstAgeItem = session.query(User.class, indexName).first(x.age.eq(60));
        assertEquals("Bob", firstAgeItem.getName());

        //No-one is aged 15, so we should get null
        User firstDefaultItem = session.query(User.class, indexName).firstOrDefault(x.age.eq(15));
        assertNull(firstDefaultItem);
      }
    }
  }

  @Test
  public void can_perform_Single_and_SingleOrDefault_Query() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";
      try (IDocumentSession session = store.openSession()) {
        addData(session);

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Age} ");
        indexDefinitionBuilder.getIndexes().put(x.name, FieldIndexing.ANALYZED);

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();

        User singleItem = session.query(User.class, indexName)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .single(x.name.eq("James"));
        assertEquals(25, singleItem.getAge());
        assertEquals("James", singleItem.getName());

        //A default query should return for results, so Single() should throw
        try {
          session.query(User.class, indexName).single();
          fail();
        } catch (IllegalStateException e) {
          //ok
        }

        //A query of age = 30 should return for 2 results, so Single() should throw
        try {
          session.query(User.class, indexName).single(x.age.eq(30));
          fail();
        } catch (IllegalStateException e) {
          //ok
        }

        //A query of age = 30 should return for 2 results, so SingleOrDefault() should also throw
        try {
          session.query(User.class, indexName).singleOrDefault(x.age.eq(30));
          fail();
        } catch (IllegalStateException e) {
          //ok
        }

        //A query of age = 75 should return for NO results, so SingleOrDefault() should return a default value
        User singleOrDefaultItem = session.query(User.class, indexName).singleOrDefault(x.age.eq(75));
        assertNull(singleOrDefaultItem);

      }
    }
  }

  @Test
  public void can_perform_boolean_queries() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";

      try (IDocumentSession session = store.openSession()) {

        User user1 = new User();
        user1.setName("Matt");
        user1.setInfo("Male Age 25");
        session.store(user1);

        User user2 = new User();
        user2.setName("Matt");
        user2.setInfo("Male Age 28");
        user2.setActive(true);
        session.store(user2);

        User user3 = new User();
        user3.setName("Matt");
        user3.setInfo("Male Age 35");
        user3.setActive(false);
        session.store(user3);
        session.saveChanges();

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Age, doc.Active} ");
        indexDefinitionBuilder.getIndexes().put(x.name, FieldIndexing.ANALYZED);

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();
        waitForNonStaleIndexes(store.getDatabaseCommands());

        IRavenQueryable<User> testQuery = session.query(User.class, indexName).where(x.name.eq("Matt").and(x.active));
        assertEquals(1, testQuery.toList().size());
        for (User testResult : testQuery) {
          assertTrue(testResult.isActive());
        }

        testQuery = session.query(User.class, indexName).where(x.name.eq("Matt").and(x.active.eq(true)));
        assertEquals(1, testQuery.toList().size());
        for (User testResult : testQuery) {
          assertTrue(testResult.isActive());
        }

        testQuery = session.query(User.class, indexName).where(x.name.eq("Matt").and(x.active.not()));
        assertEquals(2, testQuery.toList().size());
        for (User testResult : testQuery) {
          assertFalse(testResult.isActive());
        }

        testQuery = session.query(User.class, indexName).where(x.name.eq("Matt").and(x.active.eq(false)));
        assertEquals(2, testQuery.toList().size());
        for (User testResult : testQuery) {
          assertFalse(testResult.isActive());
        }
      }
    }
  }

  @Test
  public void can_perform_DateTime_Comparison_Queries() throws Exception {
    Date firstTime = new Date();
    Date secondTime = DateUtils.addMonths(firstTime, 1);
    Date thirdTime = DateUtils.addMonths(secondTime, 1);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";

      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("First");
        user1.setCreated(firstTime);
        session.store(user1);

        User user2 = new User();
        user2.setName("Second");
        user2.setCreated(secondTime);
        session.store(user2);

        User user3 = new User();
        user3.setName("Third");
        user3.setCreated(thirdTime);
        session.store(user3);

        session.saveChanges();

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Created} ");

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();
        waitForNonStaleIndexes(store.getDatabaseCommands());

        assertEquals(3, session.query(User.class, indexName).toList().size());

        List<User> testQuery = session.query(User.class, indexName).where(x.created.gt(secondTime)).toList();
        assertEquals(1, testQuery.size());
        assertTrue(testQuery.get(0).getName().equals("Third"));

        testQuery = session.query(User.class, indexName).where(x.created.goe(secondTime)).toList();
        assertEquals(2, testQuery.size());
        assertTrue(from(x, testQuery).list(x.name).contains("Third"));
        assertTrue(from(x, testQuery).list(x.name).contains("Second"));

        testQuery = session.query(User.class, indexName).where(x.created.lt(secondTime)).toList();
        assertEquals(1, testQuery.size());
        assertTrue(from(x, testQuery).list(x.name).contains("First"));

        testQuery = session.query(User.class, indexName).where(x.created.loe(secondTime)).toList();
        assertEquals(2, testQuery.size());
        assertTrue(from(x, testQuery).list(x.name).contains("First"));
        assertTrue(from(x, testQuery).list(x.name).contains("Second"));

        testQuery = session.query(User.class, indexName).where(x.created.eq(secondTime)).toList();
        assertEquals(1, testQuery.size());
        assertTrue(from(x, testQuery).list(x.name).contains("Second"));
      }
    }
  }

  @Test // See issue #105 (http://github.com/ravendb/ravendb/issues/#issue/105)
  public void does_Not_Ignore_Expressions_Before_Where() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      String indexName = "UserIndex";
      try (IDocumentSession session = store.openSession()) {

        User user1 = new User();
        user1.setName("Third");
        user1.setAge(18);
        session.store(user1);

        User user2 = new User();
        user2.setName("First");
        user2.setAge(10);
        session.store(user2);

        User user3 = new User();
        user3.setName("Second");
        user3.setAge(20);
        session.store(user3);

        session.saveChanges();

        net.ravendb.tests.linq.QUser x = QUser.user;

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder();
        indexDefinitionBuilder.setMap("from doc in docs.Users select new {doc.Name, doc.Age} ");

        store.getDatabaseCommands().deleteIndex(indexName);
        store.getDatabaseCommands().putIndex(indexName, indexDefinitionBuilder, true);
        waitForAllRequestsToComplete();

        List<User> result = session
            .query(User.class, indexName)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .orderBy(x.name.asc())
            .where(x.age.goe(18))
            .toList();
        assertEquals(2, result.size());

        assertEquals("Second", result.get(0).getName());
        assertEquals("Third", result.get(1).getName());

      }
    }
  }

  @Test // See issue #145 (http://github.com/ravendb/ravendb/issues/#issue/145)
  public void can_Use_Static_Fields_In_Where_Clauses() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from info in docs.DateTimeInfos select new {info.TimeOfDay}");
      store.getDatabaseCommands().putIndex("DateTime", indexDefinition);

      Date currentTime = new Date();

      try (IDocumentSession session = store.openSession()) {
        session.store(new DateTimeInfo(DateUtils.addHours(currentTime, 1)));
        session.store(new DateTimeInfo(DateUtils.addHours(currentTime, 2)));
        session.store(new DateTimeInfo(DateUtils.addMinutes(currentTime, 1)));
        session.store(new DateTimeInfo(DateUtils.addSeconds(currentTime, 10)));
        session.saveChanges();
      }
      QUsingRavenQueryProviderTest_DateTimeInfo x = QUsingRavenQueryProviderTest_DateTimeInfo.dateTimeInfo;

      try (IDocumentSession session = store.openSession()) {
        //Just issue a blank query to make sure there are no stale results
        session.query(DateTimeInfo.class, "DateTime").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.timeOfDay.gt(currentTime)).toList();

        IRavenQueryable<DateTimeInfo> testFail = session.query(DateTimeInfo.class, "DateTime").where(x.timeOfDay.gt(new Date(0)));

        Date dt = new Date(0);
        IRavenQueryable<DateTimeInfo> testPass = session.query(DateTimeInfo.class, "DateTime").where(x.timeOfDay.gt(dt));
        assertEquals(testPass.count(), testFail.count());
      }
    }
  }

  @Test
  public void can_Use_Static_Properties_In_Where_Clauses() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from info in docs.DateTimeInfos select new {info.TimeOfDay}");
      store.getDatabaseCommands().putIndex("DateTime", indexDefinition);

      Date currentTime = new Date();

      try (IDocumentSession session = store.openSession()) {
        session.store(new DateTimeInfo(DateUtils.addDays(currentTime, 1)));
        session.store(new DateTimeInfo(DateUtils.addDays(currentTime, -1)));
        session.store(new DateTimeInfo(DateUtils.addDays(currentTime, 1)));
        session.saveChanges();
      }
      QUsingRavenQueryProviderTest_DateTimeInfo x = QUsingRavenQueryProviderTest_DateTimeInfo.dateTimeInfo;

      try (IDocumentSession session = store.openSession()) {
        //Just issue a blank query to make sure there are no stale results
        session.query(DateTimeInfo.class, "DateTime").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.timeOfDay.gt(currentTime)).toList();

        int count = session.query(DateTimeInfo.class, "DateTime").where(x.timeOfDay.gt(new Date())).count();
        assertEquals(2, count);
      }
    }
  }

  @Test  // See issue #145 (http://github.com/ravendb/ravendb/issues/#issue/145)
  public void can_use_inequality_to_compare_dates() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from info in docs.DateTimeInfos select new {info.TimeOfDay}");
      store.getDatabaseCommands().putIndex("DateTime", indexDefinition);

      Date currentTime = new Date();

      Date date1 = DateUtils.addHours(currentTime, 1);

      try (IDocumentSession session = store.openSession()) {
        session.store(new DateTimeInfo(date1));
        session.store(new DateTimeInfo(DateUtils.addHours(currentTime, 2)));
        session.store(new DateTimeInfo(DateUtils.addMinutes(currentTime, 1)));
        session.store(new DateTimeInfo(DateUtils.addSeconds(currentTime, 10)));
        session.saveChanges();
      }
      QUsingRavenQueryProviderTest_DateTimeInfo x = QUsingRavenQueryProviderTest_DateTimeInfo.dateTimeInfo;

      try (IDocumentSession session = store.openSession()) {
        //Just issue a blank query to make sure there are no stale results
        session.query(DateTimeInfo.class, "DateTime").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.timeOfDay.gt(currentTime)).toList();

        List<DateTimeInfo> list = session.query(DateTimeInfo.class, "DateTime").where(x.timeOfDay.ne(new Date(0))).toList();
        assertTrue(list.size() > 0);

        list = session.query(DateTimeInfo.class, "DateTime").where(x.timeOfDay.ne(date1)).toList();
        assertEquals(3, list.size());
      }
    }
  }

  @Test // See issue #91 http://github.com/ravendb/ravendb/issues/issue/91 and
  //discussion here http://groups.google.com/group/ravendb/browse_thread/thread/3df57d19d41fc21
  public void can_do_projection_in_query_result() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from order in docs.Orders from line in order.Lines select new {Cost = line.Cost}");
      indexDefinition.getStores().put("Cost", FieldStorage.YES);

      store.getDatabaseCommands().putIndex("ByLineCost", indexDefinition);


      try (IDocumentSession session = store.openSession()) {
        Order order1 = new Order();
        OrderItem item1 = new OrderItem();
        item1.setCost(1.59);
        item1.setQuantity(5);

        OrderItem item2 = new OrderItem();
        item2.setCost(7.59);
        item2.setQuantity(3);

        order1.setLines(Arrays.asList(item1, item2));

        Order order2 = new Order();
        OrderItem item3 = new OrderItem();
        item3.setCost(0.59);
        item3.setQuantity(9);

        order2.setLines(Arrays.asList(item3));

        session.store(order1);
        session.store(order2);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        //  Just issue a blank query to make sure there are no stale results
        session.query(SomeDataProjection.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();

        //This is the lucene query we want to mimic
        List<SomeDataProjection> luceneResult = session.advanced().documentQuery(OrderItem.class, "ByLineCost")
            .where("Cost_Range:{Dx1 TO NULL}")
            .selectFields(SomeDataProjection.class)
            .toList();

        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;
        List<SomeDataProjection> projectionResult = session.query(OrderItem.class, "ByLineCost")
            .where(x.cost.gt(1))
            .select(SomeDataProjection.class)
            .toList();

        assertEquals(luceneResult.size(), projectionResult.size());

        int counter = 0;
        for (SomeDataProjection item : luceneResult) {
          assertEquals(item.getCost(), projectionResult.get(counter).getCost(), 0.001);
          counter++;
        }
      }
    }
  }

  @Test
  public void throws_exception_when_overloaded_distinct_called() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        OrderItem order1 = new OrderItem();
        order1.setDescription("Test");
        order1.setCost(10.0);
        session.store(order1);

        OrderItem order2 = new OrderItem();
        order2.setDescription("Test1");
        order2.setCost(10.0);
        session.store(order2);

        OrderItem order3 = new OrderItem();
        order3.setDescription("Test1");
        order3.setCost(10.0);
        session.store(order3);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        // since we don't have overridden version of distinct we test plain distinct here
        session.query(OrderItem.class).distinct().toList();
      }
    }
  }

  @QueryEntity
  public static class SomeDataProjection {
    private double cost;

    public double getCost() {
      return cost;
    }

    public void setCost(double cost) {
      this.cost = cost;
    }
  }

  public static class Order {
    private String id;
    private List<OrderItem> lines;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public List<OrderItem> getLines() {
      return lines;
    }
    public void setLines(List<OrderItem> lines) {
      this.lines = lines;
    }
  }

  public static enum Origin {
    AFRICA, UNITED_STATES;
  }

  @QueryEntity
  public static class OrderItem {
    private UUID id;
    private UUID customerId;
    private double cost;
    private double quantity;
    private Origin country;
    private String description;
    public UUID getId() {
      return id;
    }
    public void setId(UUID id) {
      this.id = id;
    }
    public UUID getCustomerId() {
      return customerId;
    }
    public void setCustomerId(UUID customerId) {
      this.customerId = customerId;
    }
    public double getCost() {
      return cost;
    }
    public void setCost(double cost) {
      this.cost = cost;
    }
    public double getQuantity() {
      return quantity;
    }
    public void setQuantity(double quantity) {
      this.quantity = quantity;
    }
    public Origin getCountry() {
      return country;
    }
    public void setCountry(Origin country) {
      this.country = country;
    }
    public String getDescription() {
      return description;
    }
    public void setDescription(String description) {
      this.description = description;
    }
  }

  @QueryEntity
  public static class DateTimeInfo {
    private String id;
    private Date timeOfDay;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public Date getTimeOfDay() {
      return timeOfDay;
    }
    public void setTimeOfDay(Date timeOfDay) {
      this.timeOfDay = timeOfDay;
    }
    public DateTimeInfo(Date timeOfDay) {
      super();
      this.timeOfDay = timeOfDay;
    }
    public DateTimeInfo() {
      super();
    }
  }

  private User firstUser = new User();
  private User lastUser = new User();

  private void addData(IDocumentSession session) {
    firstUser = new User();
    firstUser.setName("Alan");
    firstUser.setAge(30);

    lastUser = new User();
    lastUser.setName("Zoe");
    lastUser.setAge(30);

    session.store(firstUser);
    User u2 = new User();
    u2.setName("James");
    u2.setAge(25);
    session.store(u2);
    User u3 = new User();
    u3.setName("Bob");
    u3.setAge(60);
    session.store(u3);

    session.store(lastUser);
    session.saveChanges();
  }

  @Test
  public void can_Use_In_Array_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<OrderItem> items = session.query(OrderItem.class)
            .where(x.quantity.in(3.0, 5.0))
            .toList();

        assertEquals(3, items.size());
      }
    }
  }

  @Test
  public void can_Use_Strings_In_Array_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        orderItem1.setDescription("First");
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        orderItem2.setDescription("Second");
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        orderItem3.setDescription("Third");
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        orderItem4.setDescription("Fourth");
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<OrderItem> items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.description.in("", "First"))
            .toList();

        assertEquals(1, items.size());
      }

      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<OrderItem> items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.description.in("First", ""))
            .toList();

        assertEquals(1, items.size());
      }
    }
  }

  @Test
  public void can_Use_Enums_In_Array_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        orderItem1.setCountry(Origin.AFRICA);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        orderItem2.setCountry(Origin.AFRICA);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        orderItem3.setCountry(Origin.UNITED_STATES);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        orderItem4.setCountry(Origin.AFRICA);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<OrderItem> items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.country.in(Origin.UNITED_STATES))
            .toList();

        assertEquals(1, items.size());

        items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.country.in(Origin.AFRICA))
            .toList();

        assertEquals(3, items.size());
      }
    }
  }

  @Test
  public void can_Use_Enums_In_IEnumerable_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        orderItem1.setCountry(Origin.AFRICA);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        orderItem2.setCountry(Origin.AFRICA);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        orderItem3.setCountry(Origin.UNITED_STATES);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        orderItem4.setCountry(Origin.AFRICA);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<Origin> list = new ArrayList<>();
        list.add(Origin.AFRICA);

        List<OrderItem> items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.country.in(list))
            .toList();

        assertEquals(3, items.size());

      }
    }
  }

  @Test
  public void can_Use_In_IEnumerable_In_Where_Clause_with_negation() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5.0);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3.0);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<Double> list = new ArrayList<>();
        list.add(3.0);
        list.add(5.0);

        List<OrderItem> items = session.query(OrderItem.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.quantity.notIn(list))
            .toList();

        assertEquals(1, items.size());

      }
    }
  }

  @Test
  public void can_Use_In_Params_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<OrderItem> items = session.query(OrderItem.class)
            .where(x.quantity.in(3.0, 5.0))
            .toList();

        assertEquals(3, items.size());
      }
    }
  }

  @Test
  public void can_Use_In_IEnumerable_In_Where_Clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        session.store(orderItem4);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem x = QUsingRavenQueryProviderTest_OrderItem.orderItem;

        List<Double> list = new ArrayList<>();
        list.add(3.0);
        list.add(5.0);

        List<OrderItem> items = session.query(OrderItem.class)
            .where(x.quantity.in(list))
            .toList();

        assertEquals(3, items.size());
      }
    }
  }

  @Test
  public void can_Use_In_IEnumerable_Not_In_Where_Clause_on_Id() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      UUID guid1 = UUID.randomUUID();
      UUID guid2 = UUID.randomUUID();
      UUID customerId = UUID.randomUUID();

      try (IDocumentSession session = store.openSession()) {
        OrderItem orderItem1 = new OrderItem();
        orderItem1.setId(UUID.randomUUID());
        orderItem1.setCustomerId(customerId);
        orderItem1.setCost(1.59);
        orderItem1.setQuantity(5);
        session.store(orderItem1);

        OrderItem orderItem2 = new OrderItem();
        orderItem2.setId(guid1);
        orderItem2.setCustomerId(customerId);
        orderItem2.setCost(7.59);
        orderItem2.setQuantity(3);
        session.store(orderItem2);

        OrderItem orderItem3 = new OrderItem();
        orderItem3.setId(guid2);
        orderItem3.setCustomerId(customerId);
        orderItem3.setCost(1.59);
        orderItem3.setQuantity(4);
        session.store(orderItem3);

        OrderItem orderItem4 = new OrderItem();
        orderItem4.setId(UUID.randomUUID());
        orderItem4.setCustomerId(customerId);
        orderItem4.setCost(1.39);
        orderItem4.setQuantity(3);
        session.store(orderItem4);

        session.saveChanges();
      }

      List<UUID> list = new ArrayList<>();
      list.add(guid1);

      try (IDocumentSession session = store.openSession()) {
        QUsingRavenQueryProviderTest_OrderItem item = QUsingRavenQueryProviderTest_OrderItem.orderItem;
        List<OrderItem> items = session.query(OrderItem.class)
          .where(item.quantity.gt(4).and(item.customerId.eq(customerId)).and(item.id.notIn(list)))
          .toList();

        assertEquals(1, items.size());
      }
    }
  }



}
