package raven.client.linq;

import java.util.List;
import java.util.Map.Entry;

import com.mysema.query.types.Expression;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Reference;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQueryCustomization;
import raven.client.RavenQueryHighlightings;
import raven.client.RavenQueryStatistics;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.IRavenQueryInspector;
import raven.client.document.InMemoryDocumentSessionOperations;
import raven.client.indexes.AbstractTransformerCreationTask;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

//TODO: finish me
public class RavenQueryInspector<T> implements IRavenQueryable<T>, IRavenQueryInspector {

  public RavenQueryInspector(Class<T> clazz, IRavenQueryProvider provider, RavenQueryStatistics queryStats, RavenQueryHighlightings highlightings, String indexName, Expression<?> expression, InMemoryDocumentSessionOperations session, IDatabaseCommands databaseCommands, boolean isMapReduce) {

  }

  @Override
  public IRavenQueryable<T> where(Predicate predicate) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<T> toList() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Lazy<List<T>> lazily() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getIndexQueried() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IDatabaseCommands getDatabaseCommands() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InMemoryDocumentSessionOperations getSession() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Entry<String, String> getLastEqualityTerm() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IndexQuery getIndexQuery() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FacetResults getFacets(String facetSetupDoc, int start, Integer pageSize) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FacetResults getFacets(List<Facet> facets, int start, Integer pageSize) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRavenQueryable<T> statistics(Reference<RavenQueryStatistics> stats) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRavenQueryable<T> customize(Action1<IDocumentQueryCustomization> action) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <S> IRavenQueryable<S> transformWith(Class< ? extends AbstractTransformerCreationTask> transformerClazz, Class<S> resultClass) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRavenQueryable<T> addQueryInput(String name, RavenJToken value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRavenQueryable<T> spatial(Path< ? > path, Function1<SpatialCriteriaFactory, SpatialCriteria> clause) {
    // TODO Auto-generated method stub
    return null;
  }
}
