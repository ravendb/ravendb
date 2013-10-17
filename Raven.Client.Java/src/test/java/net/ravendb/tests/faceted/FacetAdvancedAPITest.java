package net.ravendb.tests.faceted;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetMode;
import net.ravendb.tests.faceted.QFacetAdvancedAPITest_Test;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.annotations.QueryEntity;

public class FacetAdvancedAPITest {

  @QueryEntity
  public static class Test {
    private String id;
    private String manufacturer;
    private Date date;
    private double cost;
    private int quantity;
    private double price;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getManufacturer() {
      return manufacturer;
    }
    public void setManufacturer(String manufacturer) {
      this.manufacturer = manufacturer;
    }
    public Date getDate() {
      return date;
    }
    public void setDate(Date date) {
      this.date = date;
    }
    public double getCost() {
      return cost;
    }
    public void setCost(double cost) {
      this.cost = cost;
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

  @org.junit.Test
  public void canUseNewAPIToDoMultipleQueries() throws Exception {
    Facet oldFacet1 = new Facet();
    oldFacet1.setName("Manufacturer");

    Facet oldFacet2 = new Facet();
    oldFacet2.setName("Cost_Range");
    oldFacet2.setMode(FacetMode.RANGES);
    oldFacet2.setRanges(Arrays.asList("[NULL TO Dx200]",
        "[Dx200 TO Dx400]",
        "[Dx400 TO Dx600]",
        "[Dx600 TO Dx800]",
        "[Dx800 TO NULL]"));

    Facet oldFacet3 = new Facet();
    oldFacet3.setName("Price_Range");
    oldFacet3.setMode(FacetMode.RANGES);
    oldFacet3.setRanges(Arrays.asList("[NULL TO Dx9.99]",
        "[Dx9.99 TO Dx49.99]",
        "[Dx49.99 TO Dx99.99]",
        "[Dx99.99 TO NULL]"));

    QFacetAdvancedAPITest_Test x = QFacetAdvancedAPITest_Test.test;

    Facet newFacet1 = new Facet();
    newFacet1.setName(x.manufacturer);
    Facet newFacet2 = new Facet();
    newFacet2.setName(x.cost);
    newFacet2.setRanges(x.cost.lt(200),
        x.cost.gt(200).and(x.cost.lt(400)),
        x.cost.gt(400).and(x.cost.lt(600)),
        x.cost.gt(600).and(x.cost.lt(800)),
        x.cost.gt(800));

    Facet newFacet3 = new Facet();
    newFacet3.setName(x.price);
    newFacet3.setRanges(x.price.lt(9.99),
        x.price.gt(9.99).and(x.price.lt(49.99)),
        x.price.gt(49.99).and(x.price.lt(99.99)),
        x.price.gt(99.99));


    assertTrue(areFacetsEqual(oldFacet1, newFacet1));
    assertTrue(areFacetsEqual(oldFacet2, newFacet2));
    assertTrue(areFacetsEqual(oldFacet3, newFacet3));

  }

  @org.junit.Test
  public void newAPIThrowsExceptionsForInvalidExpressions() {
    QFacetAdvancedAPITest_Test x = QFacetAdvancedAPITest_Test.test;

    //Create an invalid lambda and check it throws an exception!!
    try {
      Facet facet = new Facet();
      facet.setName(x.cost);
      //Ranges can be a single item or && only
      facet.setRanges(x.cost.gt(200).or(x.cost.lt(400)));

      fail();
    } catch (IllegalArgumentException e) {
      //ok
    }

    try {
      Facet facet = new Facet();
      facet.setName(x.cost);
      //Ranges can be > or < only
      facet.setRanges(x.cost.eq(200.0));
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      Facet facet = new Facet();
      //Facets must contain a Name expression
      facet.setRanges(x.cost.gt(200));
      fail();
    } catch (IllegalStateException e) {
      // ok
    }

    try {
      Facet facet =new Facet();
      facet.setName(x.cost);
      facet.setRanges(x.price.gt(9.99).and(x.cost.gt(49.99)));
      fail();
    } catch (IllegalArgumentException e) {
      //ok
    }
  }

  private Date makeDate(int year, int month, int day) {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    cal.set(year, month - 1, day);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    //NOTE: don't use DateUtils becuase it removes time zone info!

    return cal.getTime();
  }


  @org.junit.Test
  public void advancedAPIAdvancedEdgeCases() {
    QFacetAdvancedAPITest_Test x = QFacetAdvancedAPITest_Test.test;
    Date testDateTime = makeDate(2001, 12, 5);

    Facet facet = new Facet();
    facet.setName(x.date);
    facet.setRanges(x.date.lt(new Date()), x.date.gt(makeDate(2010, 12, 5)).and(x.date.lt(testDateTime)));

    assertEquals(2, facet.getRanges().size());
    assertTrue(StringUtils.isNotEmpty(facet.getRanges().get(0)));
    assertEquals("[2010\\-12\\-05T00\\:00\\:00.0000000Z TO 2001\\-12\\-05T00\\:00\\:00.0000000Z]", facet.getRanges().get(1));

  }

  private boolean areFacetsEqual(Facet left, Facet right) {
    return left.getName().equals(right.getName())
        && left.getMode().equals(right.getMode())
        && left.getRanges().size() == right.getRanges().size()
        && left.getRanges().equals(right.getRanges());
  }



}
