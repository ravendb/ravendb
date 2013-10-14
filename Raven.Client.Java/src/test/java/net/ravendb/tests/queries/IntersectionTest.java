package net.ravendb.tests.queries;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.queries.QIntersectionTest_TShirt;
import net.ravendb.tests.queries.QIntersectionTest_TShirtType;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class IntersectionTest extends RemoteClientTest {

  @Test
  public void canPerformIntersectionQuery_Remotely() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeTest(store);
    }
  }

  @Test
  public void canPerformIntersectionQuery_Linq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndSampleData(store);
      try (IDocumentSession session = store.openSession()) {

        QIntersectionTest_TShirt t = QIntersectionTest_TShirt.tShirt;
        QIntersectionTest_TShirtType tt = QIntersectionTest_TShirtType.tShirtType;
        List<TShirt> shirts = session.query(TShirt.class, "TShirtNested")
            .orderBy(t.barcodeNumber.asc())
            .where(t.name.eq("Wolf"))
            .intersect()
            .where(t.types.any(tt.color.eq("Blue").and(tt.size.eq("Small"))))
            .intersect()
            .where(t.types.any(tt.color.eq("Gray").and(tt.size.eq("Large"))))
            .toList();

        assertEquals(6, shirts.size());
        List<Integer> barcodeNumbers = new ArrayList<>();
        for (TShirt tshirt : shirts) {
          assertEquals("Wolf", tshirt.getName());
          barcodeNumbers.add(tshirt.getBarcodeNumber());
        }

        assertArrayEquals(new Integer [] { -999, 10001, 10002, 10003, 10004, 10006}, barcodeNumbers.toArray(new Integer[0]));

      }
    }
  }

  private boolean hasGrayLarge(TShirt tShirts) {
    for (TShirtType type : tShirts.getTypes()) {
      if (type.getColor().equals("Gray") && type.getSize().equals("Large")) {
        return true;
      }
    }
    return false;
  }

  private boolean hasBlueSmall(TShirt tShirts) {
    for (TShirtType type : tShirts.getTypes()) {
      if (type.getColor().equals("Gray") && type.getSize().equals("Large")) {
        return true;
      }
    }
    return false;
  }

  private void executeTest(IDocumentStore store) throws Exception {
    createIndexAndSampleData(store);
    try (IDocumentSession s = store.openSession()) {
      //This should be BarCodeNumber = -999, 10001
      List<TShirt> resultPage1 = s.advanced().luceneQuery(TShirt.class, "TShirtNested")
          .where("Name:Wolf INTERSECT Types_Color:Blue AND Types_Size:Small INTERSECT Types_Color:Gray AND Types_Size:Large")
          .orderBy("BarcodeNumber")
          .take(2)
          .toList();
      assertEquals(2, resultPage1.size());
      List<Integer> barCodeNumbers = new ArrayList<>();
      for (TShirt tShirt: resultPage1) {
        barCodeNumbers.add(tShirt.getBarcodeNumber());
        assertEquals("Wolf", tShirt.getName());
        assertTrue(hasGrayLarge(tShirt));
        assertTrue(hasBlueSmall(tShirt));
      }

      assertArrayEquals(new Integer[] { -999, 10001}, barCodeNumbers.toArray(new Integer[0]));

      //This should be BarCodeNumber = 10001, 10002 (i.e. it spans pages 1 & 2)
      List<TShirt> resultPage1a = s.advanced().luceneQuery(TShirt.class, "TShirtNested")
          .where("Name:Wolf INTERSECT Types_Color:Blue AND Types_Size:Small INTERSECT Types_Color:Gray AND Types_Size:Large")
          .orderBy("BarcodeNumber")
          .skip(1)
          .take(2)
          .toList();
      assertEquals(2, resultPage1a.size());

      barCodeNumbers = new ArrayList<>();
      for (TShirt tShirt: resultPage1a) {
        barCodeNumbers.add(tShirt.getBarcodeNumber());
        assertEquals("Wolf", tShirt.getName());
        assertTrue(hasGrayLarge(tShirt));
        assertTrue(hasBlueSmall(tShirt));
      }
      assertArrayEquals(new Integer[] { 10001, 10002}, barCodeNumbers.toArray(new Integer[0]));

    //This should be BarCodeNumber = 10002, 10003, 10004, 10006 (But NOT 10005
      List<TShirt> resultPage2 = s.advanced().luceneQuery(TShirt.class, "TShirtNested")
      .where("Name:Wolf INTERSECT Types_Color:Blue AND Types_Size:Small INTERSECT Types_Color:Gray AND Types_Size:Large")
      .orderBy("BarcodeNumber")
      .skip(2)
      .take(10) //we should only get 4 here, want to test a page size larger than what is possible!!!!!
      .toList();
      assertEquals(4, resultPage2.size());

      barCodeNumbers = new ArrayList<>();
      for (TShirt tShirt: resultPage2) {
        barCodeNumbers.add(tShirt.getBarcodeNumber());
        assertEquals("Wolf", tShirt.getName());
        assertTrue(hasGrayLarge(tShirt));
        assertTrue(hasBlueSmall(tShirt));
      }
      assertArrayEquals(new Integer[] { 10002, 10003, 10004, 10006}, barCodeNumbers.toArray(new Integer[0]));

    }

  }

  private void createIndexAndSampleData(IDocumentStore store) throws Exception {
    try (IDocumentSession session = store.openSession()) {
      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from s in docs.TShirts " +
          " from t in s.Types " +
          " select new { s.Name, Types_Color = t.Color, Types_Size = t.Size, s.BarcodeNumber }");
      indexDefinition.getSortOptions().put("BarcodeNumber", SortOptions.INT);
      store.getDatabaseCommands().putIndex("TShirtNested", indexDefinition);

      for (TShirt tShirt : getSampleData()) {
        session.store(tShirt);
      }
      session.saveChanges();
      waitForNonStaleIndexes(store.getDatabaseCommands());
    }
  }

  private List<TShirt> getSampleData() {
    TShirt tShirt1 = new TShirt(null, "Wolf", 10001, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Black", "Small"),
        new TShirtType("Black", "Medium"),
        new TShirtType("Gray", "Large")
        ));

    TShirt tShirt2 = new TShirt(null, "Wolf", 1, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Black", "Large"),
        new TShirtType("Gray", "Medium")
        ));

    TShirt tShirt3 = new TShirt(null, "Owl", 99999, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Medium")
        ));

    TShirt tShirt4 = new TShirt(null, "Wolf", -999, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Black", "Small"),
        new TShirtType("Black", "Medium"),
        new TShirtType("Gray", "Large")
        ));

    TShirt tShirt5 = new TShirt(null, "Wolf", 10002, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Large")
        ));

    TShirt tShirt6 = new TShirt(null, "Wolf", 10003, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Large")
        ));

    TShirt tShirt7 = new TShirt(null, "Wolf", 10004, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Large")
        ));

    TShirt tShirt8 = new TShirt(null, "Wolf", 10005, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Medium")  //Doesn't MAtch SUB-QUERIES
        ));

    TShirt tShirt9 = new TShirt(null, "Wolf", 10006, Arrays.asList(
        new TShirtType("Blue", "Small"),
        new TShirtType("Gray", "Large")
        ));

    return Arrays.asList(tShirt1, tShirt2, tShirt3,
        tShirt4, tShirt5, tShirt6,
        tShirt7, tShirt8, tShirt9);

  }

  @QueryEntity
  public static class TShirt {
    private String id;
    private String name;
    private int barcodeNumber;
    private List<TShirtType> types;
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
    public int getBarcodeNumber() {
      return barcodeNumber;
    }
    public void setBarcodeNumber(int barcodeNumber) {
      this.barcodeNumber = barcodeNumber;
    }
    public List<TShirtType> getTypes() {
      return types;
    }
    public void setTypes(List<TShirtType> types) {
      this.types = types;
    }
    public TShirt(String id, String name, int barcodeNumber, List<TShirtType> types) {
      super();
      this.id = id;
      this.name = name;
      this.barcodeNumber = barcodeNumber;
      this.types = types;
    }
    public TShirt() {
      super();
    }


  }

  @QueryEntity
  public static class TShirtType {

    public TShirtType() {
      super();
    }
    public TShirtType(String color, String size) {
      super();
      this.color = color;
      this.size = size;
    }
    private String color;
    private String size;
    public String getColor() {
      return color;
    }
    public void setColor(String color) {
      this.color = color;
    }
    public String getSize() {
      return size;
    }
    public void setSize(String size) {
      this.size = size;
    }
    @Override
    public String toString() {
      return "TShirtType [color=" + color + ", size=" + size + "]";
    }

  }

}
