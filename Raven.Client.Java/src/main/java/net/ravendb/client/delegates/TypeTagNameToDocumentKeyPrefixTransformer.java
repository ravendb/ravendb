package net.ravendb.client.delegates;


public interface TypeTagNameToDocumentKeyPrefixTransformer {
  public String transform(String tag);
}
