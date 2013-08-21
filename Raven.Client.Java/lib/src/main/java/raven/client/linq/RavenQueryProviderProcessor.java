package raven.client.linq;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysema.query.types.Expression;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.IDocumentQueryCustomization;
import raven.client.document.IAbstractDocumentQuery;

/**
 * Process a Linq expression to a Lucene query
 *
 * @param <T>
 */
public class RavenQueryProviderProcessor<T> {
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
  //TODO: protected void VisitExpression(Expression expression)

  public IDocumentQuery<T> getLuceneQueryFor(Expression<?> expression) {
    return null; //TODO:
  }

  public Object execute(Expression<?> expression) {
    return null;//TODO:
  }
  //TODO finish me

}
