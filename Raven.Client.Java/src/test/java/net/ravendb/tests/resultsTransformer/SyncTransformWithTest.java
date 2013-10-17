package net.ravendb.tests.resultsTransformer;

import static org.junit.Assert.assertEquals;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.tests.resultsTransformer.QSyncTransformWithTest_MyModel;

import org.junit.Test;
import com.mysema.query.annotations.QueryEntity;



public class SyncTransformWithTest extends RemoteClientTest {

  @Test
  public void canRunTransformerOnSession() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.executeTransformer(new MyTransformer());

      try (IDocumentSession session = store.openSession()) {
        MyModel model = new MyModel();
        model.setName("Sherezade");
        model.setCountry("India");
        model.setCity("Delhi");
        session.store(model);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QSyncTransformWithTest_MyModel x = QSyncTransformWithTest_MyModel.myModel;
        MyModelProjection model = session.query(MyModel.class)
          .search(x.name, "Sherezade")
          .transformWith(MyTransformer.class, MyModelProjection.class)
          .firstOrDefault();
        assertEquals("Sherezade", model.getName());
        assertEquals("India,Delhi", model.getCountryAndCity());
      }
    }
  }

  @QueryEntity
  public static class MyModel {
    private String id;
    private String name;
    private String country;
    private String city;

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

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }
  }

  public static class MyModelProjection {
    private String id;
    private String name;
    private String countryAndCity;

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

    public String getCountryAndCity() {
      return countryAndCity;
    }

    public void setCountryAndCity(String countryAndCity) {
      this.countryAndCity = countryAndCity;
    }
  }

  public static class MyTransformer extends AbstractTransformerCreationTask {
    public MyTransformer() {
      transformResults = "from d in results select new { d.Id, d.Name, CountryAndCity = String.Join(\",\", d.Country, d.City)}";
    }
  }
}
