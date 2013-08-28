package raven.client.linq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.ConstantImpl;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Order;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathImpl;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.BooleanOperation;
import com.mysema.query.types.expr.SimpleOperation;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.Constants;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.IDocumentQueryCustomization;
import raven.client.WhereParams;
import raven.client.document.DocumentQuery;
import raven.client.document.DocumentQueryCustomiation;
import raven.client.document.IAbstractDocumentQuery;
import raven.client.linq.LinqPathProvider.Result;
import raven.linq.dsl.LinqOps;

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


  static final Set<Class<?>> requireOrderByToUseRange = new HashSet<Class<?>>();
  static {
    requireOrderByToUseRange.add(int.class);
    requireOrderByToUseRange.add(long.class);
    requireOrderByToUseRange.add(float.class);
    requireOrderByToUseRange.add(double.class);
  }

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
    //TODO: finish me
    if (expression instanceof Operation) {
      if (expression instanceof BooleanOperation) {
        visitBooleanOperation((BooleanOperation) expression);
      } else {
        visitOperation((Operation<?>)expression);
      }
    } else if (expression instanceof Constant) {
      if ("root".equals(((Constant<?>) expression).getConstant())) {
        // we have root node - just skip it
        return;
      } else {
        throw new IllegalArgumentException("Expression is not supported:" + expression);
      }
    } else {
      throw new IllegalArgumentException("Expression is not supported:" + expression);
    }

  }

  private void visitOperation(Operation<?> expression) {
    if (expression.getOperator().getId().startsWith(LinqOps.Query.QUERY_OPERATORS_PREFIX)) {
      visitQueryableMethodCall(expression);
      //TODO: finish me
    } else {
      throw new IllegalArgumentException("Expression is not supported");
    }
  }

  private void visitQueryableMethodCall(Operation< ? > expression) {
    //TODO finish me
    String operatorId = expression.getOperator().getId();
    if (operatorId.equals(LinqOps.Query.WHERE.getId())) {
      insideWhere++;
      visitExpression(expression.getArg(0));
      if (chanedWhere) {
        luceneQuery.andAlso();
        luceneQuery.openSubclause();
      }
      if (chanedWhere == false && insideWhere > 1) {
        luceneQuery.openSubclause();
      }
      visitExpression(expression.getArg(1));
      if (chanedWhere == false && insideWhere > 1) {
        luceneQuery.closeSubclause();
      }
      if (chanedWhere) {
        luceneQuery.closeSubclause();
      }
      chanedWhere = true;
      insideWhere--;
    } else if (operatorId.equals(LinqOps.Query.ORDER_BY.getId())) {
      visitExpression(expression.getArg(0));
      Expression< ? > orderSpecExpression = expression.getArg(1);
      if (orderSpecExpression instanceof Constant) {
        Object constant = ((Constant) orderSpecExpression).getConstant();
        visitOrderBy((OrderSpecifier<?>[])constant);
      } else {
        throw new IllegalStateException("Constant expected in: " + expression);
      }
    } else {
      throw new IllegalStateException("Unhandled expression: " + expression);
    }
    // TODO Auto-generated method stub

  }

  private void visitOrderBy(OrderSpecifier< ? >[] orderSpecs) {
    for (OrderSpecifier<?> orderSpec : orderSpecs) {
      ExpressionInfo result = getMemberDirect(orderSpec.getTarget());
      Class<?> fieldType = result.getClazz();
      String fieldName = result.getPath();

      if (result.getMaybeProperty() != null && queryGenerator.getConventions().getFindIdentityProperty().apply(result.getMaybeProperty())) {
        fieldName = Constants.DOCUMENT_ID_FIELD_NAME;
        fieldType = String.class;
      }

      if (requireOrderByToUseRange.contains(fieldType)) {
        fieldName += "_Range";
      }
      luceneQuery.addOrder(fieldName, orderSpec.getOrder() == Order.DESC, fieldType);

    }
  }

  private void visitBooleanOperation(BooleanOperation expression) {
    if (expression.getOperator().equals(Ops.EQ)) {
      visitEquals(expression);
      //TODO: finish me
    } else {
      throw new IllegalArgumentException("Expression is not supported");
    }
  }

  private void visitEquals(BooleanOperation expression) {
    Constant<?> constantExpression = null;
    if (expression.getArg(1) instanceof Constant<?>) {
      constantExpression = (Constant< ? >) expression.getArg(1);
    }
    if (constantExpression != null && Boolean.TRUE.equals(constantExpression.getConstant())) {
      visitExpression(expression.getArg(0));
      return ;
    }

    if (constantExpression != null && Boolean.FALSE.equals(constantExpression.getConstant())) { //TODO: and expression.Left.NodeType != ExpressionType.MemberAccess
      luceneQuery.openSubclause();
      luceneQuery.where("*:*");
      luceneQuery.andAlso();
      luceneQuery.negateNext();
      visitExpression(expression.getArg(0));
      luceneQuery.closeSubclause();
      return;
    }

    if (!isMemberAccessForQuerySource(expression.getArg(0)) && isMemberAccessForQuerySource(expression.getArg(1))) {
      visitEquals((BooleanOperation) Expressions.booleanOperation(Ops.EQ, expression.getArg(1), expression.getArg(0)));
      return ;
    }

    ExpressionInfo memberInfo = getMember(expression.getArg(0));

    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue(getValueFromExpression(expression.getArg(1), getMemberType(memberInfo)));
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(true);
    whereParams.setNestedPath(memberInfo.isNestedPath());
    luceneQuery.whereEquals(whereParams);

  }

  private Object getValueFromExpression(Expression< ? > expression, Class< ? > type) {
    return linqPathProvider.getValueFromExpression(expression, type);
  }

  private Class<?> getMemberType(ExpressionInfo memberInfo) {
    return memberInfo.getClazz();
  }

  private boolean isMemberAccessForQuerySource(Expression< ? > arg) {
    //TODO: parameter
    if (!(arg instanceof Path<?>)) {
      return false;
    }
    return true;
  }

  /**
   * Gets member info for the specified expression and the path to that expression
   * @param expression
   * @return
   */
  protected ExpressionInfo getMember(Expression<?> expression) {
    //TODO: get parameter expression
    return getMemberDirect(expression);
  }

  private ExpressionInfo getMemberDirect(Expression< ? > expression) {
    Result result = linqPathProvider.getPath(expression);

    //for standard queries, we take just the last part. But for dynamic queries, we take the whole part
    result.setPath(result.getPath().substring(result.getPath().indexOf('.') + 1));
    //TODO: result.Path = castingRemover.Replace(result.Path, ""); // removing cast remains

    //TODO:array length
    String propertyName = indexName == null  || indexName.toLowerCase().startsWith("dynamic/")
        ? queryGenerator.getConventions().getFindPropertyNameForDynamicIndex().apply(clazz, indexName, currentPath, result.getPath())
            : queryGenerator.getConventions().getFindPropertyNameForIndex().apply(clazz, indexName, currentPath, result.getPath());

        ExpressionInfo expressionInfo = new ExpressionInfo(propertyName, result.getMemberType(), result.isNestedPath());
        expressionInfo.setMaybeProperty(result.getMaybeProperty());
        return expressionInfo;
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

  @SuppressWarnings("unchecked")
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
    List<TProjection> list = null;
    switch (queryType)
    {
    case FIRST:
      return finalQuery.first();
    case FIRST_OR_DEFAULT:
      return finalQuery.first();
    case SINGLE:
      list = finalQuery.toList();
      if (list.size() != 1) {
        throw new IllegalStateException("Expected one result. Got: " + list.size());
      }
      return list.get(0);
    case SINGLE_OR_DEFAULT:
      list = finalQuery.toList();
      if (list.size() > 1) {
        throw new IllegalStateException("Expected one result. Got: " + list.size());
      }
      return list.isEmpty() ? null : list.get(0);
    case ALL:
      //TODO:
      //        var pred = predicate.Compile();
      //        return finalQuery.AsQueryable().All(projection => pred((T) (object) projection));
      return null;
    case ANY:
      //TODO: return finalQuery.Any();
      return null;
    case COUNT:
      return finalQuery.getQueryResult().getTotalResults();
    case LONG_COUNT:
      return (long)finalQuery.getQueryResult().getTotalResults();
    default:
      return finalQuery;
    }
  }


}
