package net.ravendb.tests.queries;

import static com.mysema.query.collections.CollQueryFactory.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.queries.QIncludesTest_Order;
import net.ravendb.tests.queries.QIncludesTest_Order2;
import net.ravendb.tests.queries.QIncludesTest_Order3;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class IncludesTest extends RemoteClientTest {

  @Test
  public void can_use_includes_within_multi_load() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer customer1 = new Customer();
        customer1.setId("users/1");
        customer1.setName("Daniel Lang");

        Customer customer2 = new Customer();
        customer2.setId("users/2");
        customer2.setName("Oren Eini");

        Order order1 = new Order();
        order1.setCustomerId("users/1");
        order1.setNumber("1");

        Order order2 = new Order();
        order2.setCustomerId("users/1");
        order2.setNumber("2");

        Order order3 = new Order();
        order3.setCustomerId("users/2");
        order3.setNumber("3");

        session.store(customer1);
        session.store(customer2);

        session.store(order1);
        session.store(order2);
        session.store(order3);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order o = QIncludesTest_Order.order;
        List<Order> orders = session.query(Order.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfLastWrite())
            .include(o.customerId)
            .toList();

        assertEquals(3, orders.size());
        assertEquals(1, session.advanced().getNumberOfRequests());

        Customer[] customers = session.load(Customer.class,from(o, orders).list(o.customerId));
        assertEquals(3, customers.length);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }

  }

  @Test
  public void can_include_by_primary_string_property() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer customer = new Customer();
        customer.setId("customers/1");
        Order order = new Order();
        order.setCustomerId("customers/1");

        session.store(customer);
        session.store(order, "orders/1234");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order x = QIncludesTest_Order.order;
        Order order = session.include(x.customerId).load(Order.class, "orders/1234");

        // this will not require querying the server!
        Customer cust = session.load(Customer.class, order.getCustomerId());
        assertNotNull(cust);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_primary_valuetype_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer2 customer = new Customer2();
        customer.setId(1);
        session.store(customer);
        Order2 order = new Order2();
        order.setCustomer2Id(1);
        session.store(order, "orders/1234");
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order2 x = QIncludesTest_Order2.order2;
        Order2 order = session.include(Customer2.class, x.customer2Id).load(Order2.class, "orders/1234");
        // this will not require querying the server!
        Customer2 cust = session.load(Customer2.class, order.customer2Id);
        assertNotNull(cust);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_query_with_include_by_primary_string_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer customer1 = new Customer();
        customer1.setId("customers/1");
        customer1.setName("1");
        session.store(customer1);

        Customer customer2 = new Customer();
        customer2.setId("customers/2");
        customer2.setName("2");
        session.store(customer2);

        Customer customer3 = new Customer();
        customer3.setId("customers/3");
        customer3.setName("3");
        session.store(customer3);

        Order order1 = new Order();
        order1.setCustomerId("customers/1");
        order1.setTotalPrice(200);
        session.store(order1, "orders/1234");

        Order order2 = new Order();
        order2.setCustomerId("customers/2");
        order2.setTotalPrice(50);
        session.store(order2, "orders/1235");

        Order order3 = new Order();
        order3.setCustomerId("customers/3");
        order3.setTotalPrice(300);
        session.store(order3, "orders/1236");

        session.saveChanges();
      }
      QIncludesTest_Order x = QIncludesTest_Order.order;
      try (IDocumentSession session = store.openSession()) {
        List<Order> orders = session.query(Order.class)
            .customize(new DocumentQueryCustomizationFactory().include(x.customerId))
            .where(x.totalPrice.gt(100))
            .toList();

        assertEquals(2, orders.size());

        for (Order order : orders) {
          //this will not require querying the server!
          Customer cust = session.load(Customer.class, order.customerId);
          assertNotNull(cust);
        }
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_query_with_include_by_primary_valuetype_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer2 customer1 = new Customer2();
        customer1.setId(1);
        customer1.setName("1");
        session.store(customer1);

        Customer2 customer2 = new Customer2();
        customer2.setId(2);
        customer2.setName("2");
        session.store(customer2);

        Customer2 customer3 = new Customer2();
        customer3.setId(3);
        customer3.setName("3");
        session.store(customer3);

        Order2 order1 = new Order2();
        order1.setCustomer2Id(1);
        order1.setTotalPrice(200);
        session.store(order1, "orders/1234");

        Order2 order2 = new Order2();
        order2.setCustomer2Id(2);
        order2.setTotalPrice(50);
        session.store(order2, "orders/1235");

        Order2 order3 = new Order2();
        order3.setCustomer2Id(3);
        order3.setTotalPrice(300);
        session.store(order3, "orders/1236");

        session.saveChanges();
      }
      QIncludesTest_Order2 x = QIncludesTest_Order2.order2;

      try (IDocumentSession session = store.openSession()) {
        List<Order2> orders = session.query(Order2.class).customize(new DocumentQueryCustomizationFactory().include(Customer2.class, x.customer2Id))
            .where(x.totalPrice.gt(100)).toList();
        assertEquals(2, orders.size());

        for (Order2 order : orders) {
          // this will not require querying the server!
          Customer2 cust = session.load(Customer2.class, order.customer2Id);
          assertNotNull(cust);
        }

        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_primary_list_of_strings() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Supplier supp1 = new Supplier();
        supp1.setName("1");
        session.store(supp1);

        Supplier supp2 = new Supplier();
        supp2.setName("2");
        session.store(supp2);

        Supplier supp3 = new Supplier();
        supp3.setName("3");
        session.store(supp3);

        Order order = new Order();
        order.setSupplierIds(Arrays.asList("suppliers/1", "suppliers/2", "suppliers/3"));
        session.store(order, "orders/1234");

        session.saveChanges();
      }

      QIncludesTest_Order x = QIncludesTest_Order.order;
      try(IDocumentSession session = store.openSession()) {
        Order order = session.include(x.supplierIds).load(Order.class, "orders/1234");
        assertEquals(3, order.supplierIds.size());

        for (String supplierId : order.supplierIds) {
          // this will not require querying the server!
          Supplier supp = session.load(Supplier.class, supplierId);
          assertNotNull(supp);
        }

        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_primary_list_of_valuetypes() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Supplier2 supp1 = new Supplier2();
        supp1.setId(UUID.randomUUID());
        supp1.setName("1");
        session.store(supp1);

        Supplier2 supp2 = new Supplier2();
        supp2.setId(UUID.randomUUID());
        supp2.setName("2");
        session.store(supp2);

        Supplier2 supp3 = new Supplier2();
        supp3.setId(UUID.randomUUID());
        supp3.setName("3");
        session.store(supp3);

        Order2 order = new Order2();
        order.setSupplier2Ids(Arrays.asList(supp1.getId(), supp2.getId(), supp3.getId()));
        session.store(order, "orders/1234");
        session.saveChanges();
      }

      QIncludesTest_Order2 x = QIncludesTest_Order2.order2;
      try (IDocumentSession session = store.openSession()) {
        Order2 order = session.include(Supplier2.class, x.supplier2Ids).load(Order2.class, "orders/1234");
        assertEquals(3, order.getSupplier2Ids().size());

        for (UUID supplier2ID : order.getSupplier2Ids()) {
          // this will not require querying the server!
          Supplier2 supp2 = session.load(Supplier2.class, supplier2ID);
          assertNotNull(supp2);
        }

        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_secondary_string_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new Customer());
        Order order = new Order();
        Referral refferal = new Referral();
        refferal.setCustomerId("customers/1");
        order.setRefferal(refferal);
        session.store(order, "orders/1234");
        session.saveChanges();
      }

      QIncludesTest_Order x = QIncludesTest_Order.order;
      try (IDocumentSession session = store.openSession()) {
        Order order = session.include(x.refferal.customerId).load(Order.class, "orders/1234");
        // this will not require querying the server!
        Customer referrer = session.load(Customer.class, order.getRefferal().getCustomerId());
        assertNotNull(referrer);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_secondary_valuetype_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer2 customer = new Customer2();
        customer.setId(1);
        session.store(customer);

        Referral2 referral = new Referral2();
        referral.setCustomer2Id(1);
        Order2 order = new Order2();
        order.setRefferal2(referral);
        session.store(order, "orders/1234");

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order2 x = QIncludesTest_Order2.order2;
        Order2 order = session.include(Customer2.class, x.refferal2.customer2Id).load(Order2.class, "orders/1234");
        // this will not require querying the server!
        Customer2 referrer2 = session.load(Customer2.class, order.getRefferal2().getCustomer2Id());
        assertNotNull(referrer2);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_list_of_secondary_string_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Product prod1 = new Product();
        prod1.setName("1");
        session.store(prod1);

        Product prod2 = new Product();
        prod2.setName("2");
        session.store(prod2);

        Product prod3 = new Product();
        prod3.setName("3");
        session.store(prod3);

        Order order = new Order();
        LineItem lineItem1 = new LineItem();
        lineItem1.setProductId("products/1");

        LineItem lineItem2 = new LineItem();
        lineItem2.setProductId("products/2");

        LineItem lineItem3 = new LineItem();
        lineItem3.setProductId("products/3");

        order.setLineItems(Arrays.asList(lineItem1, lineItem2, lineItem3));
        session.store(order, "orders/1234");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order x = QIncludesTest_Order.order;

        Order order = session.include(x.lineItems.select().productId).load(Order.class, "orders/1234");
        for (LineItem lineItem : order.getLineItems()) {
          // this will not require querying the server!
          Product product = session.load(Product.class, lineItem.productId);
          assertNotNull(product);
        }
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_list_of_secondary_valuetype_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        UUID guid1 = UUID.randomUUID();
        UUID guid2 = UUID.randomUUID();
        UUID guid3 = UUID.randomUUID();

        Product2 prod1 = new Product2();
        prod1.setId(guid1);
        prod1.setName("1");
        session.store(prod1);

        Product2 prod2 = new Product2();
        prod2.setId(guid2);
        prod2.setName("2");
        session.store(prod2);

        Product2 prod3 = new Product2();
        prod3.setId(guid3);
        prod3.setName("3");
        session.store(prod3);

        Order2 order = new Order2();
        LineItem2 item1 = new LineItem2();
        item1.setProduct2Id(guid1);

        LineItem2 item2 = new LineItem2();
        item2.setProduct2Id(guid2);

        LineItem2 item3 = new LineItem2();
        item3.setProduct2Id(guid3);

        order.setLineItem2s(Arrays.asList(item1, item2, item3));
        session.store(order, "orders/1234");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order2 x = QIncludesTest_Order2.order2;
        Order2 order = session.include(Product2.class, x.lineItem2s.select().product2Id).load(Order2.class, "orders/1234");

        for (LineItem2 lineItem2 : order.getLineItem2s()) {
          // this will not require querying the server!
          Product2 product2 = session.load(Product2.class, lineItem2.getProduct2Id());
          assertNotNull(product2);
        }
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }
  }

  @Test
  public void can_include_by_denormalized_property() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Customer2 customer2 = new Customer2();
        customer2.setId(1);
        session.store(customer2);

        Order3 order = new Order3();
        DenormalizedCustomer customer = new DenormalizedCustomer();
        customer.setId(1);
        order.setCustomer(customer);
        session.store(order, "orders/1234");

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QIncludesTest_Order3 x = QIncludesTest_Order3.order3;
        Order3 order = session.include(Customer2.class, x.customer.id).load(Order3.class, "orders/1234");
        // this will not require querying the server!

        Customer2 fullCustomer = session.load(Customer2.class, order.customer.id);
        assertNotNull(fullCustomer);
        assertEquals(1, session.advanced().getNumberOfRequests());
      }
    }

  }

  @QueryEntity
  public static class Order {
    private String number;
    private String customerId;
    private List<String> supplierIds;
    private Referral refferal;
    private List<LineItem> lineItems;
    private double totalPrice;
    public String getNumber() {
      return number;
    }
    public void setNumber(String number) {
      this.number = number;
    }
    public String getCustomerId() {
      return customerId;
    }
    public void setCustomerId(String customerId) {
      this.customerId = customerId;
    }
    public List<String> getSupplierIds() {
      return supplierIds;
    }
    public void setSupplierIds(List<String> supplierIds) {
      this.supplierIds = supplierIds;
    }
    public Referral getRefferal() {
      return refferal;
    }
    public void setRefferal(Referral refferal) {
      this.refferal = refferal;
    }
    public List<LineItem> getLineItems() {
      return lineItems;
    }
    public void setLineItems(List<LineItem> lineItems) {
      this.lineItems = lineItems;
    }
    public double getTotalPrice() {
      return totalPrice;
    }
    public void setTotalPrice(double totalPrice) {
      this.totalPrice = totalPrice;
    }

  }

  @QueryEntity
  public static class Order2 {
    private int customer2Id;
    private List<UUID> supplier2Ids;
    private Referral2 refferal2;
    private List<LineItem2> lineItem2s;
    private double totalPrice;
    public int getCustomer2Id() {
      return customer2Id;
    }
    public void setCustomer2Id(int customer2Id) {
      this.customer2Id = customer2Id;
    }
    public List<UUID> getSupplier2Ids() {
      return supplier2Ids;
    }
    public void setSupplier2Ids(List<UUID> supplier2Ids) {
      this.supplier2Ids = supplier2Ids;
    }
    public Referral2 getRefferal2() {
      return refferal2;
    }
    public void setRefferal2(Referral2 refferal2) {
      this.refferal2 = refferal2;
    }
    public List<LineItem2> getLineItem2s() {
      return lineItem2s;
    }
    public void setLineItem2s(List<LineItem2> lineItem2s) {
      this.lineItem2s = lineItem2s;
    }
    public double getTotalPrice() {
      return totalPrice;
    }
    public void setTotalPrice(double totalPrice) {
      this.totalPrice = totalPrice;
    }

  }

  @QueryEntity
  public static class Order3 {
    private DenormalizedCustomer customer;
    private List<String> suuplierIds;
    private Referral refferal;
    private List<LineItem> lineItems;
    private double totalPrice;
    public DenormalizedCustomer getCustomer() {
      return customer;
    }
    public void setCustomer(DenormalizedCustomer customer) {
      this.customer = customer;
    }
    public List<String> getSuuplierIds() {
      return suuplierIds;
    }
    public void setSuuplierIds(List<String> suuplierIds) {
      this.suuplierIds = suuplierIds;
    }
    public Referral getRefferal() {
      return refferal;
    }
    public void setRefferal(Referral refferal) {
      this.refferal = refferal;
    }
    public List<LineItem> getLineItems() {
      return lineItems;
    }
    public void setLineItems(List<LineItem> lineItems) {
      this.lineItems = lineItems;
    }
    public double getTotalPrice() {
      return totalPrice;
    }
    public void setTotalPrice(double totalPrice) {
      this.totalPrice = totalPrice;
    }

  }

  @QueryEntity
  public static class Customer {
    private String id;
    private String name;
    private String address;
    private short age;
    private String hashedPassword;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAddress() {
      return address;
    }
    public void setAddress(String address) {
      this.address = address;
    }
    public short getAge() {
      return age;
    }
    public void setAge(short age) {
      this.age = age;
    }
    public String getHashedPassword() {
      return hashedPassword;
    }
    public void setHashedPassword(String hashedPassword) {
      this.hashedPassword = hashedPassword;
    }

  }

  @QueryEntity
  public static class Customer2 {
    private int id;
    private String name;
    private String address;
    private short age;
    private String hashedPassword;
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAddress() {
      return address;
    }
    public void setAddress(String address) {
      this.address = address;
    }
    public short getAge() {
      return age;
    }
    public void setAge(short age) {
      this.age = age;
    }
    public String getHashedPassword() {
      return hashedPassword;
    }
    public void setHashedPassword(String hashedPassword) {
      this.hashedPassword = hashedPassword;
    }

  }

  @QueryEntity
  public static class DenormalizedCustomer {
    private int id;
    private String name;
    private String address;
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAddress() {
      return address;
    }
    public void setAddress(String address) {
      this.address = address;
    }

  }

  @QueryEntity
  public static class Supplier {
    private String name;
    private String address;
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAddress() {
      return address;
    }
    public void setAddress(String address) {
      this.address = address;
    }
  }

  @QueryEntity
  public static class Supplier2 {
    private UUID id;
    private String name;
    private String address;
    public UUID getId() {
      return id;
    }
    public void setId(UUID id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAddress() {
      return address;
    }
    public void setAddress(String address) {
      this.address = address;
    }

  }

  @QueryEntity
  public static class Referral {
    private String customerId;
    private double commissionPercentage;
    public String getCustomerId() {
      return customerId;
    }
    public void setCustomerId(String customerId) {
      this.customerId = customerId;
    }
    public double getCommissionPercentage() {
      return commissionPercentage;
    }
    public void setCommissionPercentage(double commissionPercentage) {
      this.commissionPercentage = commissionPercentage;
    }
  }

  @QueryEntity
  public static class Referral2 {
    private int customer2Id;
    private double commissionPercentage;
    public int getCustomer2Id() {
      return customer2Id;
    }
    public void setCustomer2Id(int customer2Id) {
      this.customer2Id = customer2Id;
    }
    public double getCommissionPercentage() {
      return commissionPercentage;
    }
    public void setCommissionPercentage(double commissionPercentage) {
      this.commissionPercentage = commissionPercentage;
    }

  }

  @QueryEntity
  public static class LineItem {
    private String productId;
    private String name;
    private int quantity;
    private double price;
    public String getProductId() {
      return productId;
    }
    public void setProductId(String productId) {
      this.productId = productId;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public int getQuantity() {
      return quantity;
    }
    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
    public double getPrice() {
      return price;
    }
    public void setPrice(double price) {
      this.price = price;
    }

  }

  @QueryEntity
  public static class LineItem2 {
    private UUID product2Id;
    private String name;
    private int quantity;
    private double price;
    public UUID getProduct2Id() {
      return product2Id;
    }
    public void setProduct2Id(UUID product2Id) {
      this.product2Id = product2Id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public int getQuantity() {
      return quantity;
    }
    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
    public double getPrice() {
      return price;
    }
    public void setPrice(double price) {
      this.price = price;
    }

  }

  @QueryEntity
  public static class Product {
    private String name;
    private List<String> images;
    private double price;


    public double getPrice() {
      return price;
    }
    public void setPrice(double price) {
      this.price = price;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public List<String> getImages() {
      return images;
    }
    public void setImages(List<String> images) {
      this.images = images;
    }

  }

  @QueryEntity
  public static class Product2 {
    private UUID id;
    private String name;
    private List<String> images;
    private double price;
    public UUID getId() {
      return id;
    }
    public void setId(UUID id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public List<String> getImages() {
      return images;
    }
    public void setImages(List<String> images) {
      this.images = images;
    }
    public double getPrice() {
      return price;
    }
    public void setPrice(double price) {
      this.price = price;
    }

  }


}
