package net.ravendb.tests.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.indexing.NumberUtil;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.queries.QCanQueryOnCustomClassTest_Order;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.Module;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.deser.std.StdScalarDeserializer;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class CanQueryOnCustomClassTest extends RemoteClientTest {

  @Test
  public void usingConverter() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getConventions().createSerializer().registerModule(setupMoneyModule());

      try (IDocumentSession session = store.openSession()) {
        session.store(new Order(new Money("$", 50.2)));
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QCanQueryOnCustomClassTest_Order x = QCanQueryOnCustomClassTest_Order.order;
        IRavenQueryable<Order> q = session.query(Order.class).where(x.value.eq(new Money("$", 50.2)));
        assertEquals("Value:$\\:50.2", q.toString());
        List<Order> orders = q.toList();
        assertTrue(orders.size() > 0);
      }

    }
  }

  private Module setupMoneyModule() {
    SimpleModule module = new SimpleModule("moneyModule", new Version(1, 0, 0, null));
    module.addDeserializer(Money.class, new MoneyDeserializer());
    module.addSerializer(Money.class, new MoneySerializer());
    return module;
  }

  @QueryEntity
  public static class Order {
    private Money value;

    public Money getValue() {
      return value;
    }

    public void setValue(Money value) {
      this.value = value;
    }

    public Order() {
      super();
    }

    public Order(Money value) {
      super();
      this.value = value;
    }

  }

  public static class Money {
    private String currency;
    private Double amount;

    public Money() {
      super();
    }
    public Money(String currency, Double amount) {
      super();
      this.currency = currency;
      this.amount = amount;
    }
    public String getCurrency() {
      return currency;
    }
    public void setCurrency(String currency) {
      this.currency = currency;
    }
    public Double getAmount() {
      return amount;
    }
    public void setAmount(Double amount) {
      this.amount = amount;
    }

  }


  public static class MoneySerializer extends SerializerBase<Money> {

    protected MoneySerializer() {
      super(Money.class);
    }

    @Override
    public void serialize(Money value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
      jgen.writeString(value.getCurrency() + ":" +  NumberUtil.trimZeros(String.format(Constants.getDefaultLocale(), "%.11f",value.getAmount())));
    }

  }

  public static class MoneyDeserializer extends StdScalarDeserializer<Money> {

    protected MoneyDeserializer() {
      super(Money.class);
    }

    @Override
    public Money deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      JsonToken t = jp.getCurrentToken();
      if (t== JsonToken.VALUE_STRING) {
        String text = jp.getText();
        String[] tokens = text.split(":");
        Money m = new Money();
        m.setCurrency(tokens[0]);
        m.setAmount(Double.valueOf(tokens[1]));

        return m;
      }
      throw ctxt.weirdStringException(Money.class, "Can't parse money token");

    }


  }

}
