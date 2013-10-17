package net.ravendb.client.util;

import net.ravendb.abstractions.data.Etag;

public interface ILastEtagHolder {
  void updateLastWrittenEtag(Etag etag);
  Etag getLastWrittenEtag();
}
