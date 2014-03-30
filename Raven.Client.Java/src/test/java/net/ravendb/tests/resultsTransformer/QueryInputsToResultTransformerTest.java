package net.ravendb.tests.resultsTransformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.LoadConfigurationFactory;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.tests.resultsTransformer.QueryInputsToResultTransformerTest.ProductWithQueryInput.Result;

import org.junit.Test;


public class QueryInputsToResultTransformerTest extends RemoteClientTest {
  public static class Product {
    private String id;
    private String name;
    private String categoryId;

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

    public String getCategoryId() {
      return categoryId;
    }

    public void setCategoryId(String categoryId) {
      this.categoryId = categoryId;
    }

  }

  public static class Category {
    private String id;
    private String name;

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
  }

  public static class ProductWithQueryInput extends AbstractTransformerCreationTask {
    public static class Result {
      private String productId;
      private String productName;
      private String input;

      public String getProductId() {
        return productId;
      }

      public void setProductId(String productId) {
        this.productId = productId;
      }

      public String getProductName() {
        return productName;
      }

      public void setProductName(String productName) {
        this.productName = productName;
      }

      public String getInput() {
        return input;
      }

      public void setInput(String input) {
        this.input = input;
      }

    }

    public ProductWithQueryInput() {
      transformResults  = "from product in results"
        + " select new { ProductId = product.Id, ProductName = product.Name, Input = this.Query(\"input\") }";
    }
  }


  public static class ProductWithQueryInputAndInclude extends AbstractTransformerCreationTask {
    public static class Result {
      private String productId;
      private String productName;
      private String categoryId;

      public String getProductId() {
        return productId;
      }

      public void setProductId(String productId) {
        this.productId = productId;
      }

      public String getCategoryId() {
        return categoryId;
      }

      public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
      }


      public String getProductName() {
        return productName;
      }


      public void setProductName(String productName) {
        this.productName = productName;
      }

    }

    public ProductWithQueryInputAndInclude() {
      transformResults  = "from product in results"
        + " let _ = Include(product.CategoryId) "
        + " select new { ProductId = product.Id, ProductName = product.Name, product.CategoryId}";
    }
  }

  @Test
  public void canUseResultsTransformerWithQueryOnLoad() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new ProductWithQueryInput().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Product p1 = new Product();
        p1.setId("products/1");
        p1.setName("Irrelevant");

        session.store(p1);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        Result result = session.load(ProductWithQueryInput.class, ProductWithQueryInput.Result.class, "products/1",
          new LoadConfigurationFactory().addQueryParam("input", "Foo"));
        assertEquals("Foo", result.getInput());
      }
    }
  }

  @Test
  public void canUseResultsTransformerWithQueryWithRemoteDatabase() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new ProductWithQueryInput().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Product p1 = new Product();
        p1.setName("Irrelevant");

        session.store(p1);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        Result result = session.query(Product.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .transformWith(ProductWithQueryInput.class, ProductWithQueryInput.Result.class)
          .addQueryInput("input", "Foo")
          .single();

        assertEquals("Foo", result.getInput());
      }
    }
  }

  @Test
  public void canUseResultTransformerToLoadValueOnNonStoreFieldUsingQuery() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new ProductWithQueryInput().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Product p1 = new Product();
        p1.setName("Irrelevant");

        session.store(p1);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        Result result = session.query(Product.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .transformWith(ProductWithQueryInput.class, ProductWithQueryInput.Result.class)
          .addQueryInput("input", "Foo")
          .single();

        assertEquals("Irrelevant", result.getProductName());
      }
    }
  }

  @Test
  public void canUseResultsTransformerWithInclude() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new ProductWithQueryInputAndInclude().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Product p1 = new Product();
        p1.setName("Irrelevant");
        p1.setCategoryId("Category/1");
        session.store(p1);

        Category category = new Category();
        category.setId("Category/1");
        category.setName("don't know");
        session.store(category);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        ProductWithQueryInputAndInclude.Result result = session.query(Product.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .transformWith(ProductWithQueryInputAndInclude.class, ProductWithQueryInputAndInclude.Result.class)
          .single();
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result);
        Category category = session.load(Category.class, result.getCategoryId());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(category);

      }
    }
  }
}
