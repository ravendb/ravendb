package net.ravendb.client.document;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.connection.SerializationHelper;


public abstract class HiLoKeyGeneratorBase {

  protected final String RAVEN_KEY_GENERATORS_HILO = "Raven/Hilo/";
  protected final String RAVEN_KEY_SERVER_PREFIX = "Raven/ServerPrefixForHilo";

  protected final String tag;
  protected long capacity;
  protected long baseCapacity;
  private volatile RangeValue range;

  protected String lastServerPrefix;
  protected long lastRequestedUtc1, lastRequestedUtc2;

  private boolean disableCapacityChanges;

  protected HiLoKeyGeneratorBase(String tag, long capacity) {
    this.tag = tag;
    this.capacity = capacity;
    baseCapacity = capacity;
    this.range = new RangeValue(1, 0);
  }

  protected String getDocumentKeyFromId(DocumentConvention convention, long nextId) {
    return tag + convention.getIdentityPartsSeparator()+lastServerPrefix+nextId;
  }

  protected long getMaxFromDocument(JsonDocument document, long minMax) {
    long max;
    if (document.getDataAsJson().containsKey("ServerHi")) { // convert from hi to max
      Long hi = document.getDataAsJson().value(Long.TYPE, "ServerHi");
      max = ((hi - 1) * capacity);
      document.getDataAsJson().remove("ServerHi");
      document.getDataAsJson().add("Max", new RavenJValue(max));
    }
    max = document.getDataAsJson().value(Long.TYPE, "Max");
    return Math.max(max, minMax);
  }

  protected String getHiLoDocumentKey() {
    return RAVEN_KEY_GENERATORS_HILO + tag;
  }

  public boolean isDisableCapacityChanges() {
    return disableCapacityChanges;
  }

  public void setDisableCapacityChanges(boolean disableCapacityChanges) {
    this.disableCapacityChanges = disableCapacityChanges;
  }

  protected void modifyCapacityIfRequired() {
    if (disableCapacityChanges) {
      return;
    }

    long span = new Date().getTime() - lastRequestedUtc1;
    if (span < 5 * 1000) {
      span = new Date().getTime() - lastRequestedUtc2;
      if (span < 3000) {
        capacity *= 4;
      } else {
        capacity *= 2;
      }
    } else if (span > 60 * 1000) {
      capacity = Math.max(baseCapacity, capacity / 2);
    }

    lastRequestedUtc2 = lastRequestedUtc1;
    lastRequestedUtc1 = new Date().getTime();
  }

  protected JsonDocument handleGetDocumentResult(MultiLoadResult documents) {
    if (documents.getResults().size() == 2 && documents.getResults().get(1) != null) {
      lastServerPrefix = documents.getResults().get(1).value(String.class, "ServerPrefix");
    } else {
      lastServerPrefix = "";
    }
    if (documents.getResults().isEmpty() || documents.getResults().get(0) == null) {
      return null;
    }
    JsonDocument jsonDocument = SerializationHelper.toJsonDocument(documents.getResults().get(0));

    Set<String> metaKeysToRemove = new HashSet<>();
    for (String key: jsonDocument.getMetadata().getKeys()) {
      if (key.startsWith("@")) {
        metaKeysToRemove.add(key);
      }
    }
    for (String r: metaKeysToRemove) {
      jsonDocument.getMetadata().remove(r);
    }
    return jsonDocument;
  }


  protected RangeValue getRange() {
    return range;
  }

  protected void setRange(RangeValue range) {
    this.range = range;
  }


  protected static class RangeValue {
    public AtomicLong min;
    public AtomicLong max;
    public AtomicLong current;

    public RangeValue(long min, long max)
    {
      this.min = new AtomicLong(min);
      this.max = new AtomicLong(max);
      this.current = new AtomicLong(min - 1);
    }
  }
}
