package raven.client.util;

import raven.abstractions.data.Etag;

public interface ILastEtagHolder {
  void updateLastWrittenEtag(Etag etag);
  Etag getLastWrittenEtag();
}
