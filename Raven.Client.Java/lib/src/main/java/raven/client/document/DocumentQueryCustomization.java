package raven.client.document;

import java.util.Collection;
import java.util.Date;

import com.mysema.query.types.Path;

import raven.abstractions.basic.Reference;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Function2;
import raven.abstractions.data.Etag;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.indexing.SpatialOptions.SpatialRelation;
import raven.abstractions.indexing.SpatialOptions.SpatialUnits;
import raven.client.FieldHighlightings;
import raven.client.IDocumentQueryCustomization;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

public class DocumentQueryCustomization implements IDocumentQueryCustomization {
  private DocumentQuery<?> delegate;

  public DocumentQueryCustomization(DocumentQuery< ? > delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfLastWrite() {
    delegate.waitForNonStaleResultsAsOfLastWrite();
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfLastWrite(long waitTimeout) {
    delegate.waitForNonStaleResultsAsOfLastWrite(waitTimeout);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfNow() {
    delegate.waitForNonStaleResultsAsOfNow();
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfNow(long waitTimeout) {
    delegate.waitForNonStaleResultsAsOfNow(waitTimeout);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Date cutOff) {
    delegate.waitForNonStaleResultsAsOf(cutOff);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Date cutOff, long waitTimeout) {
    delegate.waitForNonStaleResultsAsOf(cutOff, waitTimeout);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Etag cutOffEtag) {
    delegate.waitForNonStaleResultsAsOf(cutOffEtag);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Etag cutOffEtag, long waitTimeout) {
    delegate.waitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResults() {
    delegate.waitForNonStaleResults();
    return this;
  }

  @Override
  public IDocumentQueryCustomization include(Path< ? > path) {
    delegate.include(path);
    return this;
  }

  @Override
  public IDocumentQueryCustomization include(String path) {
    delegate.include(path);
    return this;
  }

  @Override
  public IDocumentQueryCustomization include(Class<?> targetClass, Path< ? > path) {
    delegate.include(targetClass, path);
    return this;
  }

  @Override
  public IDocumentQueryCustomization waitForNonStaleResults(long waitTimeout) {
    delegate.waitForNonStaleResults(waitTimeout);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude) {
    delegate.withinRadiusOf(radius, latitude, longitude);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude) {
    delegate.withinRadiusOf(fieldName, radius, latitude, longitude);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    delegate.withinRadiusOf(radius, latitude, longitude, radiusUnits);
    return this;
  }

  @Override
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits) {
    delegate.withinRadiusOf(fieldName, radius, latitude, longitude, radiusUnits);
    return this;
  }

  @Override
  public IDocumentQueryCustomization relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel) {
    delegate.relatesToShape(fieldName, shapeWKT, rel);
    return this;
  }

  @Override
  public IDocumentQueryCustomization spatial(String fieldName, Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    delegate.spatial(fieldName, clause);
    return this;
  }

  @Override
  public IDocumentQueryCustomization sortByDistance() {
    delegate.sortByDistance();
    return this;
  }

  @Override
  public IDocumentQueryCustomization randomOrdering() {
    delegate.randomOrdering();
    return this;
  }

  @Override
  public IDocumentQueryCustomization randomOrdering(String seed) {
    delegate.randomOrdering(seed);
    return this;
  }

  @Override
  public IDocumentQueryCustomization beforeQueryExecution(Action1<IndexQuery> action) {
    delegate.beforeQueryExecution(action);
    return this;
  }

  @Deprecated
  @Override
  public IDocumentQueryCustomization transformResults(Function2<IndexQuery, Collection<Object>, Collection<Object>> resultsTransformer) {
    delegate.transformResults(resultsTransformer);
    return this;
  }

  @Override
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, String fragmentsField) {
    delegate.highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
    return this;
  }

  @Override
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings) {
    delegate.highlight(fieldName, fragmentLength, fragmentCount, highlightings);
    return this;
  }

  @Override
  public IDocumentQueryCustomization setHighlighterTags(String preTag, String postTag) {
    delegate.setHighlighterTags(preTag, postTag);
    return this;
  }

  @Override
  public IDocumentQueryCustomization setHighlighterTags(String[] preTags, String[] postTags) {
    delegate.setHighlighterTags(preTags, postTags);
    return this;
  }

  @Override
  public IDocumentQueryCustomization noTracking() {
    delegate.noTracking();
    return this;
  }

  @Override
  public IDocumentQueryCustomization noCaching() {
    delegate.noCaching();
    return this;
  }

  @Override
  public String toString() {
    return delegate.toString();
  }


}
