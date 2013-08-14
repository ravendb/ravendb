package raven.client.document;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.mysema.query.types.Path;
import com.mysema.query.types.path.ListPath;

import raven.abstractions.basic.Reference;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.AggregationOperation;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.indexing.SpatialOptions.SpatialRelation;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.FieldHighlightings;
import raven.client.IDocumentQuery;
import raven.client.connection.IDatabaseCommands;
import raven.client.listeners.IDocumentQueryListener;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

/**
 * A query against a Raven index
 * @param <T>
 */
public class DocumentQuery<T> extends AbstractDocumentQuery<T, DocumentQuery<T>> implements IDocumentQuery<T> {

  /**
   * Initializes a new instance of the {@link DocumentQuery} class.
   * @param session
   * @param databaseCommands
   * @param indexName
   * @param fieldsToFetch
   * @param projectionFields
   * @param queryListeners
   * @param isMapReduce
   */
  public DocumentQuery(Class<T> clazz, InMemoryDocumentSessionOperations session , IDatabaseCommands databaseCommands, String indexName,
      String[] fieldsToFetch, String[] projectionFields, List<IDocumentQueryListener> queryListeners, boolean isMapReduce) {
    super(clazz, session, databaseCommands, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce);
  }

  /**
   * Initializes a new instance of the {@link DocumentQuery} class.
   * @param other
   */
  public DocumentQuery(DocumentQuery<T> other) {
    super(other);
  }


  ///TODO: finish me

}
