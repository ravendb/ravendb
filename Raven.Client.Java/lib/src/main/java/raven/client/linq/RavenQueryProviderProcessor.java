package raven.client.linq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysema.query.types.Expression;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.IDocumentQueryCustomization;
import raven.client.document.DocumentQuery;
import raven.client.document.DocumentQueryCustomiation;
import raven.client.document.IAbstractDocumentQuery;

/**
 * Process a Linq expression to a Lucene query
 *
 * @param <T>
 */
public class RavenQueryProviderProcessor<T> {
  private Class<T> clazz;
  private final Action1<IDocumentQueryCustomization> customizeQuery;
  protected final IDocumentQueryGenerator queryGenerator;
  private final Action1<QueryResult> afterQueryExecuted;
  private boolean chanedWhere;
  private int insideWhere;
  private IAbstractDocumentQuery<T> luceneQuery;
  private Expression<?> predicate;
  private SpecialQueryType queryType = SpecialQueryType.NONE;
  private Class<?> newExpressionType;
  private String currentPath = "";
  private int subClauseDepth;
  private String resultsTransformer;
  private final Map<String, RavenJToken> queryInputs;

  private LinqPathProvider linqPathProvider;

  protected final String indexName;

  private Set<String> fieldsToFetch;
  private List<RenamedField> fieldsToRename;

  private boolean insideSelect;
  private final boolean isMapReduce;

  /**
   * Gets the current path in the case of expressions within collections
   * @return
   */
  public String getCurrentPath() {
    return currentPath;
  }

  public RavenQueryProviderProcessor(Class<T> clazz, IDocumentQueryGenerator queryGenerator, Action1<IDocumentQueryCustomization> customizeQuery,
      Action1<QueryResult> afterQueryExecuted, String indexName, Set<String> fieldsToFetch, List<RenamedField> fieldsToRename, boolean isMapReduce,
      String resultsTransformer, Map<String, RavenJToken> queryInputs) {
    this.clazz = clazz;
    this.fieldsToFetch = fieldsToFetch;
    this.fieldsToRename = fieldsToRename;
    newExpressionType = clazz;
    this.queryGenerator = queryGenerator;
    this.indexName = indexName;
    this.isMapReduce = isMapReduce;
    this.afterQueryExecuted = afterQueryExecuted;
    this.customizeQuery = customizeQuery;
    this.resultsTransformer = resultsTransformer;
    this.queryInputs = queryInputs;
    linqPathProvider = new LinqPathProvider(queryGenerator.getConventions());
  }

  public Set<String> getFieldsToFetch() {
    return fieldsToFetch;
  }

  public void setFieldsToFetch(Set<String> fieldsToFetch) {
    this.fieldsToFetch = fieldsToFetch;
  }

  /**
   * Rename the fields from one name to another
   * @return
   */
  public List<RenamedField> getFieldsToRename() {
    return fieldsToRename;
  }

  /**
   * Rename the fields from one name to another
   * @param fieldsToRename
   */
  public void setFieldsToRename(List<RenamedField> fieldsToRename) {
    this.fieldsToRename = fieldsToRename;
  }
  /**
   * Visits the expression and generate the lucene query
   */
  protected void visitExpression(Expression<?> expression) {
    //TODO:
  }

  @SuppressWarnings("unchecked")
  public IDocumentQuery<T> getLuceneQueryFor(Expression<?> expression) {
    IDocumentQuery<T> q = queryGenerator.luceneQuery(clazz, indexName, isMapReduce);
    luceneQuery = (IAbstractDocumentQuery<T>) q;

    visitExpression(expression);
    if (customizeQuery != null) {
      customizeQuery.apply(new DocumentQueryCustomiation((DocumentQuery< ? >) luceneQuery));
    }
    return q.selectFields(clazz, fieldsToFetch.toArray(new String[0]));
  }

  public Object execute(Expression<?> expression) {
    chanedWhere = false;

    luceneQuery = (IAbstractDocumentQuery<T>) getLuceneQueryFor(expression);
    if (newExpressionType.equals(clazz)) {
      return executeQuery(clazz);
    }
    /*TODOvar genericExecuteQuery = typeof (RavenQueryProviderProcessor<T>).GetMethod("ExecuteQuery", BindingFlags.Instance | BindingFlags.NonPublic);
    var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(newExpressionType);
    return executeQueryWithProjectionType.Invoke(this, new object[0]);*/
    return null; //TODO:
  }

  private <TProjection> Object executeQuery(Class<TProjection> projectionClass) {
    List<String> renamedFields = new ArrayList<>();
    for (String field :fieldsToFetch) {
      for (RenamedField renamedField : fieldsToRename) {
        if (renamedField.getOriginalField().equals(field)) {
          renamedFields.add(renamedField.getNewField());
          break;
        }
      }
      renamedFields.add(field);
    }

    IDocumentQuery<TProjection> finalQuery = ((IDocumentQuery<T>)luceneQuery).selectFields(projectionClass, fieldsToFetch.toArray(new String[0]), renamedFields.toArray(new String[0]));
    finalQuery.setResultTransformer(this.resultsTransformer);
    finalQuery.setQueryInputs(this.queryInputs);


    if (!fieldsToRename.isEmpty()) {
      finalQuery.afterQueryExecuted(new Action1<QueryResult>() {
        @Override
        public void apply(QueryResult result) {
          renameResults(result);
        }
      });
    }
    Object executeQuery = getQueryResult(finalQuery);

    QueryResult queryResult = finalQuery.getQueryResult();
    if (afterQueryExecuted != null) {
      afterQueryExecuted.apply(queryResult);
    }

    return executeQuery;
  }

  public void renameResults(QueryResult queryResult)
  {
    /*TODO
    for (int index = 0; index < queryResult.Results.Count; index++)
    {
      var result = queryResult.Results[index];
      var safeToModify = (RavenJObject) result.CreateSnapshot();
      bool changed = false;
      var values = new Dictionary<string, RavenJToken>();
      foreach (var renamedField in FieldsToRename.Select(x=>x.OriginalField).Distinct())
      {
        RavenJToken value;
        if (safeToModify.TryGetValue(renamedField, out value) == false)
          continue;
        values[renamedField] = value;
        safeToModify.Remove(renamedField);
      }
      foreach (var rename in FieldsToRename)
      {
        RavenJToken val;
        if (values.TryGetValue(rename.OriginalField, out val) == false)
          continue;
        changed = true;
        var ravenJObject = val as RavenJObject;
        if (rename.NewField == null && ravenJObject != null)
        {
          safeToModify = ravenJObject;
        }
        else if (rename.NewField != null)
        {
          safeToModify[rename.NewField] = val;
        }
        else
        {
          safeToModify[rename.OriginalField] = val;
        }
      }
      if (!changed)
        continue;
      safeToModify.EnsureCannotBeChangeAndEnableSnapshotting();
      queryResult.Results[index] = safeToModify;
    }*/
  }

  private <TProjection> Object getQueryResult(IDocumentQuery<TProjection> finalQuery) {
    /*TODO:
    switch (queryType)
    {
      case FIRST:
        return finalQuery.first();
      }
      case SpecialQueryType.FirstOrDefault:
      {
        return finalQuery.FirstOrDefault();
      }
      case SpecialQueryType.Single:
      {
        return finalQuery.Single();
      }
      case SpecialQueryType.SingleOrDefault:
      {
        return finalQuery.SingleOrDefault();
      }
      case SpecialQueryType.All:
      {
        var pred = predicate.Compile();
        return finalQuery.AsQueryable().All(projection => pred((T) (object) projection));
      }
      case SpecialQueryType.Any:
      {
        return finalQuery.Any();
      }
      case SpecialQueryType.Count:
      {
        var queryResultAsync = finalQuery.QueryResult;
        return queryResultAsync.TotalResults;
      }
      case SpecialQueryType.LongCount:
      {
        var queryResultAsync = finalQuery.QueryResult;
        return (long) queryResultAsync.TotalResults;
      }
      default:
      {
        return finalQuery;
      }
    }*/
    return finalQuery;
  }

  //TODO finish me

}
