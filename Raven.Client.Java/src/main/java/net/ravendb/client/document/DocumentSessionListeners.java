package net.ravendb.client.document;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.client.listeners.IDocumentConflictListener;
import net.ravendb.client.listeners.IDocumentConversionListener;
import net.ravendb.client.listeners.IDocumentDeleteListener;
import net.ravendb.client.listeners.IDocumentQueryListener;
import net.ravendb.client.listeners.IDocumentStoreListener;
import net.ravendb.client.listeners.IExtendedDocumentConversionListener;


/**
 * Holder for all the listeners for the session
 */
public class DocumentSessionListeners {

  private List<IDocumentConversionListener> conversionListeners;
  private List<IExtendedDocumentConversionListener> extendedConversionListeners;
  private List<IDocumentQueryListener> queryListeners;
  private List<IDocumentStoreListener> storeListeners;
  private List<IDocumentDeleteListener> deleteListeners;
  private List<IDocumentConflictListener> conflictListeners;

  public DocumentSessionListeners() {
    conversionListeners = new ArrayList<>();
    extendedConversionListeners = new ArrayList<>();
    queryListeners = new ArrayList<>();
    storeListeners = new ArrayList<>();
    deleteListeners = new ArrayList<>();
    conflictListeners = new ArrayList<>();
  }

  public List<IDocumentConversionListener> getConversionListeners() {
    return conversionListeners;
  }
  public void setConversionListeners(List<IDocumentConversionListener> conversionListeners) {
    this.conversionListeners = conversionListeners;
  }
  public List<IExtendedDocumentConversionListener> getExtendedConversionListeners() {
    return extendedConversionListeners;
  }
  public void setExtendedConversionListeners(List<IExtendedDocumentConversionListener> extendedConversionListeners) {
    this.extendedConversionListeners = extendedConversionListeners;
  }
  public List<IDocumentQueryListener> getQueryListeners() {
    return queryListeners;
  }
  public void setQueryListeners(List<IDocumentQueryListener> queryListeners) {
    this.queryListeners = queryListeners;
  }
  public List<IDocumentStoreListener> getStoreListeners() {
    return storeListeners;
  }
  public void setStoreListeners(List<IDocumentStoreListener> storeListeners) {
    this.storeListeners = storeListeners;
  }
  public List<IDocumentDeleteListener> getDeleteListeners() {
    return deleteListeners;
  }
  public void setDeleteListeners(List<IDocumentDeleteListener> deleteListeners) {
    this.deleteListeners = deleteListeners;
  }
  public List<IDocumentConflictListener> getConflictListeners() {
    return conflictListeners;
  }
  public void setConflictListeners(List<IDocumentConflictListener> conflictListeners) {
    this.conflictListeners = conflictListeners;
  }

}
