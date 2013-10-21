package net.ravendb.client.util;

import net.ravendb.abstractions.data.Etag;

public class GlobalLastEtagHolder implements ILastEtagHolder {

  private static class EtagHolder {
    public Etag etag;

    public EtagHolder(Etag etag) {
      super();
      this.etag = etag;
    }

  }

  private volatile EtagHolder lastEtag;
  protected final Object lastEtagLocker = new Object();

  @Override
  public void updateLastWrittenEtag(Etag etag) {
    if (etag == null) {
      return;
    }

    if (lastEtag == null) {
      synchronized (lastEtagLocker) {
        if (lastEtag == null) {
          lastEtag = new EtagHolder(etag);
          return;
        }
      }
    }
    // not the most recent etag
    if (lastEtag.etag.compareTo(etag) >= 0) {
      return;
    }

    synchronized (lastEtagLocker) {
      // not the most recent etag
      if (lastEtag.etag.compareTo(etag) >= 0) {
        return;
      }

      lastEtag = new EtagHolder(etag);
    }
  }


  @Override
  public Etag getLastWrittenEtag() {
    EtagHolder etagHolder = lastEtag;
    if (etagHolder == null)
      return null;
    return etagHolder.etag;
  }
}
