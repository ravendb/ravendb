package net.ravendb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Currency;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import net.ravendb.abstractions.exceptions.JsonWriterException;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Test;


public class RavenJObjectsTest {

  static class Company {
    private String name;
    private List<Person> employees;

    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public List<Person> getEmployees() {
      return employees;
    }
    public void setEmployees(List<Person> employees) {
      this.employees = employees;
    }
  }

  static class ComplexObject {
    private byte[] bytes;
    private BigDecimal bigDecimal;
    private BigInteger bigInteger;
    private String string;
    private long longObj;
    private int intObj;
    private Date date;
    private Boolean bool;
    private char[] chars;
    private int[] ints;
    private UUID uuid;
    private URL url;
    private Currency currency;

    public URL getUrl() {
      return url;
    }
    public void setUrl(URL url) {
      this.url = url;
    }
    public Currency getCurrency() {
      return currency;
    }
    public void setCurrency(Currency currency) {
      this.currency = currency;
    }
    public UUID getUuid() {
      return uuid;
    }
    public void setUuid(UUID uuid) {
      this.uuid = uuid;
    }
    public int[] getInts() {
      return ints;
    }
    public void setInts(int[] ints) {
      this.ints = ints;
    }
    public byte[] getBytes() {
      return bytes;
    }
    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }
    public BigDecimal getBigDecimal() {
      return bigDecimal;
    }
    public void setBigDecimal(BigDecimal bigDecimal) {
      this.bigDecimal = bigDecimal;
    }
    public BigInteger getBigInteger() {
      return bigInteger;
    }
    public void setBigInteger(BigInteger bigInteger) {
      this.bigInteger = bigInteger;
    }
    public String getString() {
      return string;
    }
    public void setString(String string) {
      this.string = string;
    }
    public long getLongObj() {
      return longObj;
    }
    public void setLongObj(long longObj) {
      this.longObj = longObj;
    }
    public int getIntObj() {
      return intObj;
    }
    public void setIntObj(int intObj) {
      this.intObj = intObj;
    }
    public Date getDate() {
      return date;
    }
    public void setDate(Date date) {
      this.date = date;
    }
    public Boolean getBool() {
      return bool;
    }
    public void setBool(Boolean bool) {
      this.bool = bool;
    }
    public char[] getChars() {
      return chars;
    }
    public void setChars(char[] chars) {
      this.chars = chars;
    }

  }

  static class Person {
    private String name;
    private String surname;
    private int[] types;


    public int[] getTypes() {
      return types;
    }
    public void setTypes(int[] types) {
      this.types = types;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getSurname() {
      return surname;
    }
    public void setSurname(String surname) {
      this.surname = surname;
    }
  }

  @Test
  public void testRavenJValue() throws Exception {
    RavenJValue stringValue = new RavenJValue("this is string");
    assertEquals("this is string", stringValue.getValue());
    assertEquals(JTokenType.STRING, stringValue.getType());

    RavenJValue intValue = new RavenJValue(5);
    assertEquals(Integer.valueOf(5), intValue.getValue());
    assertEquals(JTokenType.INTEGER, intValue.getType());

    RavenJValue longValue = new RavenJValue(5L);
    assertEquals(Long.valueOf(5), longValue.getValue());
    assertEquals(JTokenType.INTEGER, longValue.getType());

    RavenJValue doubleValue = new RavenJValue((double)12.23f);
    assertEquals(Double.valueOf(12.23f), doubleValue.getValue());
    assertEquals(JTokenType.FLOAT, doubleValue.getType());

    RavenJValue floatValue = new RavenJValue(12.23f);
    assertEquals(12.23f, (float)floatValue.getValue(), 0.001f);
    assertEquals(JTokenType.FLOAT, floatValue.getType());

    RavenJValue booleanValue = new RavenJValue(true);
    assertTrue((boolean)booleanValue.getValue());
    assertEquals(JTokenType.BOOLEAN, booleanValue.getType());

  }

  @Test
  public void testRavenJValueTypeDetection() throws Exception {
    Object o = null;
    assertEquals(JTokenType.NULL, new RavenJValue(o).getType());
    assertEquals(JTokenType.STRING, new RavenJValue("string").getType());
    assertEquals(JTokenType.INTEGER, new RavenJValue(JTokenType.BOOLEAN).getType());
    assertEquals(JTokenType.FLOAT, new RavenJValue(Float.valueOf(12.34f)).getType());
    assertEquals(JTokenType.FLOAT, new RavenJValue(Double.valueOf(12.34f)).getType());
    assertEquals(JTokenType.DATE, new RavenJValue(new Date()).getType());
    assertEquals(JTokenType.BOOLEAN, new RavenJValue(Boolean.FALSE).getType());
    assertEquals(JTokenType.STRING, new RavenJValue(new URI("http://ravendb.net")).getType());
    assertEquals(JTokenType.BYTES, new RavenJValue("test".getBytes()).getType());
    assertEquals(JTokenType.DATE, new RavenJValue(new Date()).getType());
  }


  @Test
  public void testDontIterateOnDeleted() throws Exception {
    RavenJObject obj = new RavenJObject();
    obj.add("Id", new RavenJValue(5));
    obj.remove("Id");
    int count = 0;
    for (@SuppressWarnings("unused") Entry<String, RavenJToken> prop: obj) {
      count ++;
    }
    assertEquals(0, count);
  }



  @Test
  public void testHashCodeAndEqual() throws Exception {

    RavenJArray array1 = RavenJArray.parse("[1,2,3,4]");
    RavenJArray array2 = RavenJArray.parse("[1,2,3]");
    assertNotEquals(array1, array2);
    assertNotEquals(array1.hashCode(), array2.hashCode());
    array2.add(RavenJToken.fromObject(4));
    assertEquals(array1, array2);
    assertEquals(array1.hashCode(), array2.hashCode());

    assertTrue(RavenJToken.deepEquals(null, null));
    assertFalse(RavenJToken.deepEquals(array1, null));
    assertFalse(RavenJToken.deepEquals(null, array2));

    assertEquals(0, RavenJToken.deepHashCode(null));

    assertEquals(array1, RavenJToken.fromObject(array1));

    RavenJObject complexObject = RavenJObject.parse("{ \"a\" : 5, \"b\" : [1,2,3], \"c\" : { \"d\" : null} , \"d\" : null, \"e\": false, \"f\" : \"string\"}");
    RavenJObject complexObject2 = RavenJObject.parse("{ \"a\" : 5, \"b\" : [1,2,3], \"e\": false, \"c\" : { \"d\" : null} , \"d\" : null,  \"f\" : \"string\"}");
    assertEquals(complexObject, complexObject2);
    assertEquals(complexObject.hashCode(), complexObject2.hashCode());

  }

  @Test
  public void testEquality() throws URISyntaxException, MalformedURLException {
    innerEqCheck(RavenJToken.fromObject(new byte[] { 1,2,3}), RavenJToken.fromObject(new byte[] {1,2,3}));
    innerEqCheck(RavenJToken.fromObject(Base64.encodeBase64String( new byte[] { 1,2,3})), RavenJToken.fromObject(new byte[] {1,2,3}));

    assertNotEquals(RavenJToken.fromObject("AA="), RavenJToken.fromObject(new byte[] {1,2,3}));
    assertNotEquals(RavenJToken.fromObject(new byte[] {1,2,3, 4}), RavenJToken.fromObject(new byte[] {1,2,3}));

    ComplexObject complexObject1 = getComplexObject();
    ComplexObject complexObject2 = getComplexObject();
    complexObject2.setBytes("aa".getBytes());
    assertNotEquals(RavenJObject.fromObject(complexObject1), RavenJObject.fromObject(complexObject2));
    complexObject1.setBytes("aa".getBytes());
    assertNotEquals(RavenJObject.fromObject(complexObject1), RavenJObject.fromObject(complexObject2));

    innerEqCheck(RavenJToken.fromObject(new byte[] { 1,2,3}), RavenJToken.fromObject(new byte[] {1,2,3}));

    innerEqCheck(RavenJToken.fromObject(new URI("http://ravendb.net")), RavenJToken.fromObject("http://ravendb.net"));
    assertNotEquals(RavenJToken.fromObject(new URI("http://ravendb.net")), RavenJToken.fromObject("invalid_value"));

    innerEqCheck(RavenJValue.getNull(), RavenJValue.getNull());
    innerEqCheck(RavenJToken.fromObject(5), RavenJToken.fromObject(5L));
    innerEqCheck(RavenJToken.fromObject(5), RavenJToken.fromObject((short)5));
    innerEqCheck(RavenJToken.fromObject(BigInteger.TEN), RavenJToken.fromObject((short)10));
    assertNotEquals(RavenJToken.fromObject(0), RavenJToken.fromObject((short)1));
    assertNotEquals(RavenJToken.fromObject(0L), RavenJToken.fromObject((short)1));
    innerEqCheck(new RavenJValue(5) , new RavenJValue(5L));

    innerEqCheck(new RavenJValue((short)5) , new RavenJValue((short)5));
    innerEqCheck(new RavenJValue((float)5.1) , new RavenJValue((float)5.1));
    innerEqCheck(new RavenJValue((float)5.1) , new RavenJValue((float)5.10000001));

    innerEqCheck(RavenJToken.parse("123456789012345678901234567890"), RavenJToken.parse("123456789012345678901234567890"));
    assertNotEquals(RavenJToken.parse("123456789012345678901234567890"), RavenJToken.parse("23.5"));

    assertNotEquals(RavenJToken.fromObject(new BigDecimal("12345.12345")), RavenJToken.fromObject(12345.126));
    innerEqCheck(RavenJToken.fromObject(new BigDecimal("12345.1234561")), RavenJToken.fromObject(12345.1234562)); // numbers are really close to each other

    innerEqCheck(RavenJToken.fromObject(2.3f), RavenJToken.parse("2.3"));
    innerEqCheck(RavenJToken.fromObject(2.3), RavenJToken.parse("2.3"));

  }

  private void innerEqCheck(RavenJToken t1, RavenJToken t2) {
    assertEquals(t1, t2);
    assertEquals(t2, t1);
  }

  @Test
  public void testClone() throws Exception {
    RavenJValue value1 = new RavenJValue("raven is cool");
    RavenJValue clonedValue = value1.cloneToken();
    assertFalse(value1 == clonedValue); //yes, we compare using ==
    assertTrue(value1.getValue() == clonedValue.getValue());
    assertEquals(value1.getType(), clonedValue.getType());
  }

  @Test
  public void testSnapshots() throws Exception {
    RavenJValue value1=  new RavenJValue("test");
    assertFalse(value1.isSnapshot());
    value1.ensureCannotBeChangeAndEnableShapshotting();
    assertTrue(value1.isSnapshot());

    RavenJObject object = RavenJObject.parse("{ \"a\" : [ 1,2,3] } ");
    assertFalse(object.isSnapshot());
    object.ensureCannotBeChangeAndEnableShapshotting();
    assertTrue(object.isSnapshot());
    RavenJObject snapshot = object.createSnapshot();
    assertFalse(snapshot.isSnapshot());
    snapshot.ensureSnapshot("snap1");
    assertTrue(snapshot.isSnapshot());
  }

  @Test(expected = IllegalStateException.class)
  public void testSetValueOnSnapshot()  {
    RavenJValue value1=  new RavenJValue("test");
    value1.ensureCannotBeChangeAndEnableShapshotting();
    value1.setValue("aa");
  }


  @Test(expected = IllegalStateException.class)
  public void testCreateSnapshotOnInvalidObject() {
    RavenJArray array = RavenJArray.parse("[1,2,3,4]");
    array.createSnapshot();
  }

  @Test
  public void testSetValue() {
    RavenJValue value1 =  new RavenJValue("test");
    value1.setValue("test2");
    assertEquals(JTokenType.STRING, value1.getType());

    value1.setValue(null);
    assertEquals(JTokenType.NULL, value1.getType());

    value1.setValue("test");
    assertEquals(JTokenType.STRING, value1.getType());

    value1.setValue(new Date());
    assertEquals(JTokenType.INTEGER, value1.getType());

    value1.setValue(12.2f);
    assertEquals(JTokenType.FLOAT, value1.getType());
  }

  @Test
  public void testValues() {
    //RavenJObject complexObject = RavenJObject.parse("{ \"a\" : 5, \"b\" : [1,2,3], \"c\" : { \"d\" : null} , \"d\" : null, \"e\": false, \"f\" : \"string\"}");

    RavenJObject complexObject = RavenJObject.parse("{ \"a\" : 5,  \"e\": true, \"f\" : \"string\"}");

    List<String> valuesString = complexObject.values(String.class);
    assertEquals("5", valuesString.get(2));
    assertEquals("true", valuesString.get(1));
    assertEquals("string", valuesString.get(0));

    List<Integer> valuesInt = complexObject.values(Integer.class);
    assertEquals(new Integer(5), valuesInt.get(2));
    assertEquals(new Integer(1), valuesInt.get(1));
    assertEquals(new Integer(0), valuesInt.get(0));

    List<Boolean> valuesBool = complexObject.values(Boolean.class);
    assertEquals(Boolean.FALSE, valuesBool.get(2));
    assertEquals(Boolean.TRUE, valuesBool.get(1));
    assertEquals(Boolean.FALSE, valuesBool.get(0));

    complexObject = RavenJObject.parse("{ \"b\" : [1,2,3]}");
    List<RavenJArray>valuesArray = complexObject.values(RavenJArray.class);
    assertEquals(3, valuesArray.get(0).size());
    valuesInt = valuesArray.get(0).values(Integer.class);
    assertEquals(new Integer(1), valuesInt.get(0));
    assertEquals(new Integer(2), valuesInt.get(1));
    assertEquals(new Integer(3), valuesInt.get(2));

    RavenJToken token = RavenJToken.fromObject("String");
    assertEquals("String", token.value(String.class));
    token = RavenJToken.fromObject(new Integer(5));
    assertEquals(new Integer(5), token.value(Integer.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetValueException() {
    RavenJValue value1 = RavenJValue.getNull();
    value1.setValue(Calendar.getInstance());
  }

  @Test
  public void createSnapshot() {
    RavenJValue value = new RavenJValue("test");
    try {
      value.createSnapshot();
    } catch (IllegalStateException e) {
      // expected
    }
    value.ensureCannotBeChangeAndEnableShapshotting();
    RavenJValue snapshot = value.createSnapshot();
    snapshot.setValue("test2");
    assertEquals("Snapshot should not be changed!", "test", value.getValue());
  }


  @Test
  public void testParser() {
    innerTestParseRavenJValue("null", JTokenType.NULL, null);
    innerTestParseRavenJValue("true", JTokenType.BOOLEAN, true);
    innerTestParseRavenJValue("false", JTokenType.BOOLEAN, false);
    innerTestParseRavenJValue("\"ala\"", JTokenType.STRING, "ala");
    innerTestParseRavenJValue("12", JTokenType.INTEGER, 12);
    innerTestParseRavenJValue("12.5", JTokenType.FLOAT, Double.valueOf(12.5f));
    innerTestParseRavenJValue("123456789012345678901234567890", JTokenType.INTEGER, new BigInteger("123456789012345678901234567890"));
    RavenJValue jToken = (RavenJValue) RavenJToken.parse("123456789012345678901234567890.123456");
    assertEquals(Double.class, jToken.getValue().getClass());


    assertEquals(JTokenType.ARRAY, RavenJToken.parse("[12,34]").getType());
    assertEquals(JTokenType.OBJECT, RavenJToken.parse("{  \"f\" : \"string\"}").getType());

    RavenJToken testObj = RavenJToken.fromObject("testing");
    assertEquals(JTokenType.STRING, testObj.getType());
    try {
      RavenJObject.fromObject("a");
      fail("Cannot parse array as object!");
    } catch (IllegalArgumentException e)  { /* ok */ }

    try {
      RavenJObject.parse("{");
      fail("Cannot parse invalid object");
    } catch (JsonWriterException e) { /* ok */ }

    try {
      RavenJObject.parse("{ 5");
      fail("Cannot parse invalid object");
    } catch (JsonWriterException e) { /* ok */ }

    RavenJObject complexObject = RavenJObject.parse("{ \"a\" : 5, \"b\" : [1,2,3], \"c\" : { \"d\" : null} , \"d\" : null, \"e\": false, \"f\" : \"string\"}");
    assertEquals(6, complexObject.getCount());
    Set<String> props = complexObject.getKeys();
    Set<String> expectedProps = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f"));
    assertEquals(expectedProps, props);
    assertEquals(RavenJToken.parse("5"), complexObject.get("a"));
    assertEquals(JTokenType.INTEGER, complexObject.get("a").getType());
    assertEquals(JTokenType.ARRAY, complexObject.get("b").getType());
    assertEquals(JTokenType.OBJECT, complexObject.get("c").getType());
    assertEquals(JTokenType.NULL, complexObject.get("d").getType());
    assertEquals(JTokenType.BOOLEAN, complexObject.get("e").getType());
    assertEquals(JTokenType.STRING, complexObject.get("f").getType());

    RavenJObject cloneToken = complexObject.cloneToken();

    assertEquals(6, cloneToken.getCount());
    assertEquals(JTokenType.INTEGER, cloneToken.get("a").getType());
    assertEquals(JTokenType.ARRAY, cloneToken.get("b").getType());
    assertEquals(3, ((RavenJArray)cloneToken.get("b")).size());
    assertEquals(JTokenType.OBJECT, cloneToken.get("c").getType());
    assertEquals(JTokenType.NULL, cloneToken.get("d").getType());
    assertEquals(JTokenType.BOOLEAN, cloneToken.get("e").getType());
    assertEquals(JTokenType.STRING, cloneToken.get("f").getType());

    assertTrue(cloneToken.containsKey("d"));
    cloneToken.add("x", RavenJToken.fromObject(null));
    assertEquals(JTokenType.NULL, cloneToken.get("x").getType());

    cloneToken.remove("x");
    assertFalse(cloneToken.containsKey("x"));


    try {
      RavenJObject.parse("{ : 5}");
      fail("Cannot parse invalid object");
    } catch (JsonWriterException e) { /* ok */ }

  }

  @Test
  public void testObjectWrite() throws IOException {
    RavenJObject object = RavenJObject.parse("{\"a\": [1,2,3,4]}");

    ByteArrayOutputStream baos =new ByteArrayOutputStream();
    try (JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(baos)) {
      object.writeTo(jsonGenerator);
    }
    assertEquals("{\"a\":[1,2,3,4]}",baos.toString());
    assertEquals("{\"a\":[1,2,3,4]}",object.toString());

  }


  @Test
  public void testInitializeRavenJArray() {
    RavenJValue value1 = new RavenJValue("value1");
    RavenJValue value2 = new RavenJValue(5);
    RavenJValue value3 = new RavenJValue(2.5f);
    RavenJValue value4 = new RavenJValue(false);
    RavenJValue value5 = RavenJValue.getNull();

    RavenJArray array = new RavenJArray(value1, value2, value3, value4, value5);

    assertEquals(5, array.size());
    assertEquals(value3, array.get(2)); //get is 0-based

    RavenJArray emptyArray = new RavenJArray();
    assertEquals(0, emptyArray.size());

    array.set(3, new RavenJValue(true));

  }



  @Test(expected = JsonWriterException.class)
  public void testWriteArrayToCustomReader() throws IOException {

    String longString = StringUtils.repeat("a", 10000);
    RavenJArray jArray = RavenJArray.parse("[1,2,3, \"" + longString + "\"]");

    try (JsonGenerator generator = new JsonFactory().createJsonGenerator(new ByteArrayOutputStream())) {
      jArray.writeTo(generator);
    }

    jArray = RavenJArray.parse("[1,2,3, \"" + longString + "\"]");
    OutputStream outputStream = mock(OutputStream.class);
    doThrow(new IOException()).when(outputStream).write(any(byte[].class),anyInt(),anyInt());

    try (JsonGenerator generator = new JsonFactory().createJsonGenerator(outputStream)) {
      jArray.writeTo(generator);
    }

  }

  @Test
  public void testParseJArray() {
    String array1String = "[\"value1\", 5, false, null]";
    RavenJArray ravenJArray = RavenJArray.parse(array1String);

    assertEquals(4, ravenJArray.size());

    assertNotNull(ravenJArray.get(0));
    assertNotNull(ravenJArray.get(1));
    assertNotNull(ravenJArray.get(2));
    assertNotNull(ravenJArray.get(3));

    assertEquals(JTokenType.NULL, ravenJArray.get(3).getType());


    String arrayOfArray = "[ [1,2,3], false, [null, null, \"test\"]  ]";
    RavenJArray array2 = RavenJArray.parse(arrayOfArray);

    assertEquals(3, array2.size());
    assertEquals(3, ((RavenJArray) array2.get(0)).size());
    assertEquals(3, ((RavenJArray) array2.get(2)).size());

  }

  @Test
  public void testParseInvalidArray() {
    try {
      RavenJArray.parse("[ 1,2");
      fail("it was invalid array!");
    } catch (Exception e) { /* ok */ }

    try {
      RavenJArray.parse("[");
      fail("it was invalid array!");
    } catch (Exception e) { /* ok */ }

    try {
      RavenJArray.parse("1");
      fail("it wasn't array!");
    } catch (Exception e) { /* ok */ }
    try {
      RavenJArray.parse("");
      fail("it wasn't array!");
    } catch (Exception e) { /* ok */ }
    try {
      RavenJArray.parse((String)null);
      fail("it wasn't array!");
    } catch (Exception e) { /* ok */ }
  }

  @Test
  public void testArraySnapshot() {
    RavenJArray array = new RavenJArray(new RavenJValue(false), new RavenJValue("5"));
    assertFalse(array.isSnapshot());
    array.ensureCannotBeChangeAndEnableShapshotting();
    assertTrue(array.isSnapshot());
    try {
      array.add(new RavenJValue(5.5f));
      fail("Array was locked - we can add elemenets");
    } catch (Exception e) { /* ok */ }
    RavenJArray snapshot = array.createSnapshot();
    snapshot.add(RavenJValue.getNull());
    assertEquals(3, snapshot.size());

  }

  @Test
  public void testBasicArrayOps() {
    RavenJArray array = RavenJArray.parse("[ {\"t\": 5 }]");
    assertEquals(1, array.size());
    RavenJObject ravenJObject = RavenJObject.parse("{\"t\": 5 }");
    array.remove(ravenJObject);
    assertTrue(array.size() == 0);

    array.insert(0, RavenJToken.fromObject("test"));
    assertEquals(1, array.size());
    array.removeAt(0);
    assertTrue(array.size() == 0);

  }

  @Test
  public void testArrayClone() {
    RavenJArray array = new RavenJArray(new RavenJArray(new RavenJValue(5l), new RavenJValue(7l), new RavenJValue(false)));
    RavenJArray clonedToken = array.cloneToken();
    // now modify original object
    ((RavenJArray) array.get(0)).add(new RavenJValue(true));
    assertEquals(4, ((RavenJArray) array.get(0)).size());
    assertEquals(3, ((RavenJArray) clonedToken.get(0)).size());

    // now clone array with nulls

    array = new RavenJArray(RavenJValue.getNull(), RavenJValue.getNull(), new RavenJValue(true));
    clonedToken = array.cloneToken();
    assertEquals(3, clonedToken.size());
    assertEquals(JTokenType.NULL, array.get(0).getType());


  }

  private void innerTestParseRavenJValue(String input, JTokenType expectedTokenType, Object expectedValue) {
    RavenJToken ravenJToken = RavenJToken.parse(input);
    RavenJValue ravenJValue = (RavenJValue) ravenJToken;

    assertEquals(expectedTokenType, ravenJValue.getType());
    assertEquals(expectedValue, ravenJValue.getValue());

  }

  private ComplexObject getComplexObject() throws MalformedURLException {
    ComplexObject complexObject = new ComplexObject();
    complexObject.setBigDecimal(new BigDecimal("120.123456"));
    complexObject.setBigInteger(new BigInteger("1234567890"));
    complexObject.setBool(Boolean.TRUE);
    complexObject.setBytes("testing".getBytes());
    complexObject.setChars("testing".toCharArray());
    complexObject.setDate(new Date());
    complexObject.setIntObj(1234);
    complexObject.setLongObj(12345L);
    complexObject.setString("test");
    complexObject.setInts(new int[] { 1,2,3});
    complexObject.setUuid(UUID.randomUUID());
    complexObject.setCurrency(Currency.getInstance("USD"));
    complexObject.setUrl(new URL("http://ravendb.net"));

    return complexObject;
  }

  @Test
  public void testFromComplexObject() throws IOException {
    RavenJObject ravenJObject = RavenJObject.fromObject(getComplexObject());

    RavenJObject nullComplexObject = RavenJObject.fromObject(new ComplexObject());

    RavenJObject complexObjectClone = ravenJObject.cloneToken();
    assertEquals(complexObjectClone.getCount(), ravenJObject.getCount());
    for (String key: complexObjectClone.getKeys()) {
      RavenJToken token1 = ravenJObject.get(key);
      RavenJToken token2 = complexObjectClone.get(key);
      RavenJToken nullToken = nullComplexObject.get(key);
      assertEquals(key, token1.getType(), token2.getType());
      assertTrue(RavenJToken.deepEquals(token1, token2));
      assertFalse(RavenJToken.deepEquals(token1, nullToken));
      assertEquals(token1.deepHashCode(), token2.deepHashCode());
    }

    String complexToString = ravenJObject.toString();
    RavenJObject parsedComplex = RavenJObject.parse(complexToString);
    assertEquals(ravenJObject, parsedComplex);

  }


  @Test
  public void testToString() {
    assertEquals("", RavenJValue.getNull().toString());
  }

  @Test
  public void testRavenJObjectFromObject() throws JsonWriterException {
    Person person1 = new Person();
    person1.setName("Joe");
    person1.setSurname("Doe");
    person1.setTypes(new int[] { 1,2,3,4,5 });

    RavenJObject ravenJObject = RavenJObject.fromObject(person1);
    assertNotNull(ravenJObject);

  }
}
