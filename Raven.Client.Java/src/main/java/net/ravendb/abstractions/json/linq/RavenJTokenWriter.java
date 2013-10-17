package net.ravendb.abstractions.json.linq;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Stack;

import org.codehaus.jackson.Base64Variant;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonStreamContext;
import org.codehaus.jackson.ObjectCodec;

public class RavenJTokenWriter extends JsonGenerator {

  private RavenJToken token;
  private RavenJValue value;
  private Stack<RavenJToken> tokenStack = new Stack<>();

  private String tempPropName;

  protected RavenJToken getCurrentToken() {
    return (tokenStack.isEmpty()) ? null : tokenStack.peek();
  }

  public RavenJToken getToken() {
    if (token != null) {
      return token;
    }

    return value;
  }

  @Override
  public JsonGenerator enable(Feature f) {
    throw new UnsupportedOperationException("Features are not supported!");
  }

  @Override
  public JsonGenerator disable(Feature f) {
    throw new UnsupportedOperationException("Features are not supported!");
  }

  @Override
  public boolean isEnabled(Feature f) {
    throw new UnsupportedOperationException("Features are not supported!");
  }

  @Override
  public JsonGenerator setCodec(ObjectCodec oc) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public ObjectCodec getCodec() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public JsonGenerator useDefaultPrettyPrinter() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeStartArray() throws IOException, JsonGenerationException {
    addParent(new RavenJArray());
  }

  @Override
  public void writeEndArray() throws IOException, JsonGenerationException {
    removeParent();
  }

  @Override
  public void writeStartObject() throws IOException, JsonGenerationException {
    addParent(new RavenJObject());
  }

  private void addParent(RavenJToken token) throws JsonGenerationException {
    if (this.token == null) {
      this.token = token;
      this.tokenStack.push(this.token);
      return;
    }
    RavenJToken currentToken = getCurrentToken();

    switch (currentToken.getType()) {
    case OBJECT:
      ((RavenJObject) currentToken).set(tempPropName, token);
      tempPropName = null;
      break;
    case ARRAY:
      ((RavenJArray) currentToken).add(token);
      break;
    default:
      throw new JsonGenerationException("Unexpected token: " + currentToken.getType());
    }
    tokenStack.push(token);

  }

  private void addValue(Object value, JTokenType type) throws JsonGenerationException {
    addValue(new RavenJValue(value), type);
  }

  private void addValue(RavenJValue value, JTokenType type) throws JsonGenerationException {
    if (tokenStack.isEmpty()) {
      this.value = value;
    } else {
      RavenJToken currentToken = getCurrentToken();
      switch (currentToken.getType()) {
      case OBJECT:
        ((RavenJObject) currentToken).set(tempPropName, value);
        tempPropName = null;
        break;
      case ARRAY:
        ((RavenJArray) currentToken).add(value);
        break;
      default:
        throw new JsonGenerationException("Unexpected token: " + type);
      }
    }
  }

  @Override
  public void writeEndObject() throws IOException, JsonGenerationException {
    removeParent();
  }

  private void removeParent() {
    tokenStack.pop();
  }

  @Override
  public void writeFieldName(String name) throws IOException, JsonGenerationException {
    if (tempPropName != null) {
      throw new JsonGenerationException("Was not expecting a property name here");
    }
    this.tempPropName = name;
  }

  @Override
  public void writeString(String text) throws IOException, JsonGenerationException {
    if (text == null) {
      addValue(new RavenJValue(null, JTokenType.NULL), JTokenType.NULL);
    } else {
      addValue(text, JTokenType.STRING);
    }
  }

  @Override
  public void writeString(char[] text, int offset, int len) throws IOException, JsonGenerationException {
    writeString(new String(Arrays.copyOfRange(text, offset, offset + len)));
  }

  @Override
  public void writeRawUTF8String(byte[] text, int offset, int length) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeUTF8String(byte[] text, int offset, int length) throws IOException, JsonGenerationException {
    writeString(new String(Arrays.copyOfRange(text, offset, offset + length), Charset.forName("UTF-8")));
  }

  @Override
  public void writeRaw(String text) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRaw(String text, int offset, int len) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRaw(char[] text, int offset, int len) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRaw(char c) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRawValue(String text) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRawValue(String text, int offset, int len) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeRawValue(char[] text, int offset, int len) throws IOException, JsonGenerationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeBinary(Base64Variant b64variant, byte[] data, int offset, int len) throws IOException, JsonGenerationException {
    addValue(Arrays.copyOfRange(data, offset, offset + len), JTokenType.BYTES);
  }

  @Override
  public void writeNumber(int v) throws IOException, JsonGenerationException {
    addValue(v, JTokenType.INTEGER);
  }

  @Override
  public void writeNumber(long v) throws IOException, JsonGenerationException {
    addValue(v, JTokenType.INTEGER);
  }

  @Override
  public void writeNumber(BigInteger v) throws IOException, JsonGenerationException {
    addValue(v, JTokenType.INTEGER);
  }

  @Override
  public void writeNumber(double d) throws IOException, JsonGenerationException {
    addValue(d, JTokenType.FLOAT);
  }

  @Override
  public void writeNumber(float f) throws IOException, JsonGenerationException {
    addValue(f, JTokenType.FLOAT);
  }

  @Override
  public void writeNumber(BigDecimal dec) throws IOException, JsonGenerationException {
    addValue(dec, JTokenType.FLOAT);
  }

  @Override
  public void writeNumber(String encodedValue) throws IOException, JsonGenerationException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeBoolean(boolean state) throws IOException, JsonGenerationException {
    addValue(state, JTokenType.BOOLEAN);
  }

  @Override
  public void writeNull() throws IOException, JsonGenerationException {
    addValue(new RavenJValue(null, JTokenType.NULL), JTokenType.NULL);
  }

  @Override
  public void writeObject(Object pojo) throws IOException, JsonProcessingException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void writeTree(JsonNode rootNode) throws IOException, JsonProcessingException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void copyCurrentEvent(JsonParser jp) throws IOException, JsonProcessingException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void copyCurrentStructure(JsonParser jp) throws IOException, JsonProcessingException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public JsonStreamContext getOutputContext() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void flush() throws IOException {
    // do nothing
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

}
