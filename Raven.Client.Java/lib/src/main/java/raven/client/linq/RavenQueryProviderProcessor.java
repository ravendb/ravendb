package raven.client.linq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import raven.abstractions.LinqOps;
import raven.abstractions.basic.Reference;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.data.Constants;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.EscapeQueryOptions;
import raven.client.IDocumentQuery;
import raven.client.SearchOptions;
import raven.client.WhereParams;
import raven.client.document.DocumentQuery;
import raven.client.document.DocumentQueryCustomiation;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.IAbstractDocumentQuery;
import raven.client.linq.LinqPathProvider.Result;
import raven.querydsl.CollectionAnyVisitor;
import raven.querydsl.StackBasedContext;

import com.google.common.collect.Lists;
import com.mysema.query.support.Context;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Order;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PredicateOperation;
import com.mysema.query.types.expr.BooleanOperation;
import com.mysema.query.types.expr.Param;

/**
 * Process a Linq expression to a Lucene query
 *
 * @param <T>
 */
public class RavenQueryProviderProcessor<T> {
  private Class<T> clazz;
  private final DocumentQueryCustomizationFactory customizeQuery;
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

  public RavenQueryProviderProcessor(Class<T> clazz, IDocumentQueryGenerator queryGenerator, DocumentQueryCustomizationFactory customizeQuery,
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
  @SuppressWarnings("unchecked")
  protected void visitExpression(Expression<?> expression) {
    if (expression instanceof Operation) {
      if (expression instanceof BooleanOperation || expression instanceof PredicateOperation || expression.getType().equals(Boolean.class)) {
        visitBooleanOperation((Operation<Boolean>) expression);
      } else {
        visitOperation((Operation<?>)expression);
      }
    } else if (expression instanceof Constant) {
      if ("root".equals(((Constant<?>) expression).getConstant())) {
        // we have root node - just skip it
        return;
      } else {
        visitMemberAccess((Constant<?>) expression, true);
      }
    } else {
      throw new IllegalArgumentException("Expression is not supported:" + expression);
    }
  }

  private void visitBooleanOperation(Operation<Boolean> expression) {
    //TODO: support for QueryDSL between
    if (expression.getOperator().equals(Ops.OR)) {
      visitOrElse(expression);
    } else if (expression.getOperator().equals(Ops.AND)) {
      visitAndAlso(expression);
    } else if (expression.getOperator().equals(Ops.NE)) {
      visitNotEquals(expression);
    } else if (expression.getOperator().equals(Ops.EQ)) {
      visitEquals(expression);
    } else if (expression.getOperator().equals(Ops.GT)) {
      visitGreatherThan(expression);
    } else if (expression.getOperator().equals(Ops.GOE)) {
      visitGreatherThanOrEqual(expression);
    } else if (expression.getOperator().equals(Ops.LT)) {
      visitLessThan(expression);
    } else if (expression.getOperator().equals(Ops.LOE)) {
      visitLessThanOrEqual(expression);
    } else if (expression.getOperator().equals(Ops.NOT)) {
      // try match NOT_COL_IS_EMPTY
      Expression< ? > subExpr1 = expression.getArg(0);
      if (subExpr1 instanceof Operation) {
        Operation<?> subOp1 = (Operation<?>) subExpr1;
        if (subOp1.getOperator().equals(Ops.COL_IS_EMPTY)) {
          visitCollectionEmpty(subOp1.getArg(0), false);
          return;
        }
      }

      luceneQuery.openSubclause();
      luceneQuery.where("*:*");
      luceneQuery.andAlso();
      luceneQuery.negateNext();
      visitExpression(expression.getArg(0));
      luceneQuery.closeSubclause();
    } else if (expression.getOperator().equals(Ops.COL_IS_EMPTY)) {
      visitCollectionEmpty(expression.getArg(0), true);
      return ;
    } else if (expression.getOperator().equals(Ops.IN)) {
      visitContains(expression);
    } else if (expression.getOperator().equals(Ops.CONTAINS_KEY)) {
      visitContainsKey(expression);
    } else if (expression.getOperator().equals(Ops.CONTAINS_VALUE)) {
      visitContainsValue(expression);
    } else if (expression.getOperator().equals(Ops.IS_NULL)) {
      visitIsNull(expression);
    } else if (expression.getOperator().equals(Ops.STRING_CONTAINS)) {
      throw new IllegalStateException("Contains is not supported, doing a substring match over a text field is a" +
      		" very slow operation, and is not allowed using the Linq API. The recommended method is to use full text search (mark the field as Analyzed and use the search() method to query it.");
    } else {
      throw new IllegalArgumentException("Expression is not supported: " + expression.getOperator());
    }
  }


  private void visitIsNull(Operation<Boolean> expression) {

    ExpressionInfo memberInfo = getMember(expression.getArg(0));

    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue(null);
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(false);
    whereParams.setNestedPath(memberInfo.isNestedPath());
    luceneQuery.whereEquals(whereParams);
  }

  private void visitContainsValue(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    String oldPath = currentPath;
    currentPath = memberInfo.getPath() + "_Value";
    Expression< ? > keyArgument = expression.getArg(1);
    visitExpression(keyArgument);
    currentPath = oldPath;
  }

  private void visitContainsKey(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    String oldPath = currentPath;
    currentPath = memberInfo.getPath() + "_Key";
    Expression< ? > keyArgument = expression.getArg(1);
    visitExpression(keyArgument);
    currentPath = oldPath;
  }

  private void visitCollectionEmpty(Expression< ? > expression, boolean isNegated) {
    if (isNegated) {
      luceneQuery.openSubclause();
      luceneQuery.where("*:*");
      luceneQuery.andAlso();
      luceneQuery.negateNext();
    }
    ExpressionInfo memberInfo = getMember(expression);
    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue("*");
    whereParams.setAllowWildcards(true);
    whereParams.setAnalyzed(true);
    whereParams.setNestedPath(memberInfo.isNestedPath());
    luceneQuery.whereEquals(whereParams);
    if (isNegated) {
      luceneQuery.closeSubclause();
    }

  }

  private void visitAndAlso(Operation<Boolean> andAlso) {
    if (tryHandleBetween(andAlso)) {
      return;
    }
    if (subClauseDepth > 0) {
      luceneQuery.openSubclause();
    }
    subClauseDepth++;
    visitExpression(andAlso.getArg(0));
    luceneQuery.andAlso();
    visitExpression(andAlso.getArg(1));
    subClauseDepth--;
    if (subClauseDepth > 0) {
      luceneQuery.closeSubclause();
    }
  }

  private boolean tryHandleBetween(Operation<Boolean> andAlso) {
    // x.Foo > 100 && x.Foo < 200
    // x.Foo < 200 && x.Foo > 100
    // 100 < x.Foo && 200 > x.Foo
    // 200 > x.Foo && 100 < x.Foo

    Expression< ? > leftExp = andAlso.getArg(0);
    Expression< ? > rightExp = andAlso.getArg(1);

    Operation<?> left = null;
    Operation<?> right = null;
    if (leftExp instanceof Operation) {
      left = (Operation< ? >) leftExp;
    }
    if (rightExp instanceof Operation) {
      right = (Operation< ? >) rightExp;
    }

    if (left == null || right == null) {
      return false;
    }

    boolean isPossibleBetween =
        (left.getOperator().equals(Ops.GT) && right.getOperator().equals(Ops.LT)) ||
        (left.getOperator().equals(Ops.GOE) && right.getOperator().equals(Ops.LOE)) ||
        (left.getOperator().equals(Ops.LT) && right.getOperator().equals(Ops.GT)) ||
        (left.getOperator().equals(Ops.LOE) && right.getOperator().equals(Ops.GT));

    if (!isPossibleBetween) {
      return false;
    }

    Tuple<ExpressionInfo, Object> leftMember = getMemberForBetween(left);
    Tuple<ExpressionInfo, Object> rightMember = getMemberForBetween(right);

    if (leftMember == null || rightMember == null) {
      return false;
    }

    // both must be on the same property
    if (!leftMember.getItem1().getPath().equals(rightMember.getItem1().getPath())) {
      return false;
    }

    Object min = (left.getOperator().equals(Ops.LT) || left.getOperator().equals(Ops.LOE)) ? rightMember.getItem2() : leftMember.getItem2();
    Object max = (left.getOperator().equals(Ops.LT) || left.getOperator().equals(Ops.LOE)) ? leftMember.getItem2() : rightMember.getItem2();

    if (left.getOperator().equals(Ops.GOE) || left.getOperator().equals(Ops.LOE)) {
      luceneQuery.whereBetweenOrEqual(leftMember.getItem1().getPath(), min, max);
    } else {
      luceneQuery.whereBetween(leftMember.getItem1().getPath(), min, max);
    }
    return true;
  }

  private Tuple<ExpressionInfo, Object> getMemberForBetween(Operation< ? > binaryExpression) {
    if (isMemberAccessForQuerySource(binaryExpression.getArg(0))) {
      ExpressionInfo expressionInfo = getMember(binaryExpression.getArg(0));
      return Tuple.create(expressionInfo, getValueFromExpression(binaryExpression.getArg(1), expressionInfo.getClazz()));
    }
    if (isMemberAccessForQuerySource(binaryExpression.getArg(1))) {
      ExpressionInfo expressionInfo = getMember(binaryExpression.getArg(1));
      return Tuple.create(expressionInfo, getValueFromExpression(binaryExpression.getArg(0), expressionInfo.getClazz()));
    }
    return null;
  }

  private Object getValueFromExpression(Expression< ? > expression, Class< ? > type) {
    return linqPathProvider.getValueFromExpression(expression, type);
  }

  private void visitOrElse(Operation<Boolean> orElse) {
    if (subClauseDepth > 0) {
      luceneQuery.openSubclause();
    }
    subClauseDepth++;
    visitExpression(orElse.getArg(0));
    luceneQuery.orElse();
    visitExpression(orElse.getArg(1));
    subClauseDepth--;
    if (subClauseDepth > 0) {
      luceneQuery.closeSubclause();
    }

  }

  private void visitEquals(Operation<Boolean> expression) {
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


  private boolean isMemberAccessForQuerySource(Expression< ? > arg) {
    //TODO: parameter
    if (!(arg instanceof Path<?>)) {
      return false;
    }
    return true;
  }

  private void visitNotEquals(Operation<Boolean> expression) {
    //TODO: implement me
  }

  private Class<?> getMemberType(ExpressionInfo memberInfo) {
    return memberInfo.getClazz();
  }

  /**
   * Gets member info for the specified expression and the path to that expression
   * @param expression
   * @return
   */
  protected ExpressionInfo getMember(Expression<?> expression) {
    Param< ? > parameterExpression = getParameterExpressionIncludingConvertions(expression);
    if (parameterExpression != null) {
      if (currentPath.endsWith(",")) {
        currentPath = currentPath.substring(0, currentPath.length() -1);
      }
      ExpressionInfo expressionInfo = new ExpressionInfo(currentPath, parameterExpression.getType(), false);
      return expressionInfo;
    }
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

  private static Param<?> getParameterExpressionIncludingConvertions(Expression expression) {
    if (expression instanceof ParamExpression){
      return (Param< ? >) expression;
    }
    return null;
  }

  //TODO: private void VisitStringContains(MethodCallExpression _)

  //TODO: private void VisitStartsWith(MethodCallExpression expression)

  //TODO: private void VisitEndsWith(MethodCallExpression expression)

  //TODO: private void VisitIsNullOrEmpty(MethodCallExpression expression) (in query DSL we have isNull, isNotNull, isEmpty, isNotEmpty

  private void visitGreatherThan(Operation<Boolean> expression) {
    if (!isMemberAccessForQuerySource(expression.getArg(0)) &&  isMemberAccessForQuerySource(expression.getArg(1))) {
      visitLessThan((BooleanOperation) BooleanOperation.create(Ops.LT, expression.getArg(1), expression.getArg(0)));
      return;
    }
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    Object value = getValueFromExpression(expression.getArg(1), getMemberType(memberInfo));

    luceneQuery.whereGreaterThan(getFieldNameForRangeQuery(memberInfo, value), value);
  }

  private void visitGreatherThanOrEqual(Operation<Boolean> expression) {
    if (!isMemberAccessForQuerySource(expression.getArg(0)) &&  isMemberAccessForQuerySource(expression.getArg(1))) {
      visitLessThan((BooleanOperation) BooleanOperation.create(Ops.LOE, expression.getArg(1), expression.getArg(0)));
      return;
    }
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    Object value = getValueFromExpression(expression.getArg(1), getMemberType(memberInfo));

    luceneQuery.whereGreaterThanOrEqual(getFieldNameForRangeQuery(memberInfo, value), value);
  }

  private void visitLessThan(Operation<Boolean> expression) {
    if (!isMemberAccessForQuerySource(expression.getArg(0)) && isMemberAccessForQuerySource(expression.getArg(1))) {
      visitGreatherThan((BooleanOperation) BooleanOperation.create(Ops.GT, expression.getArg(1), expression.getArg(0)));
      return;
    }
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    Object value = getValueFromExpression(expression.getArg(1), getMemberType(memberInfo));

    luceneQuery.whereLessThan(getFieldNameForRangeQuery(memberInfo, value), value);
  }

  private void visitLessThanOrEqual(Operation<Boolean> expression) {
    if (!isMemberAccessForQuerySource(expression.getArg(0)) && isMemberAccessForQuerySource(expression.getArg(1))) {
      visitGreatherThan((BooleanOperation) BooleanOperation.create(Ops.GOE, expression.getArg(1), expression.getArg(0)));
      return;
    }
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    Object value = getValueFromExpression(expression.getArg(1), getMemberType(memberInfo));

    luceneQuery.whereLessThanOrEqual(getFieldNameForRangeQuery(memberInfo, value), value);
  }


  private void visitContains(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(1));
    String oldPath = currentPath;
    currentPath = memberInfo.getPath() + ",";
    Expression< ? > containsArgument = expression.getArg(0);
    visitExpression(containsArgument);
    currentPath = oldPath;
  }

  private void visitMemberAccess(Constant< ? > expression, boolean boolValue) {
    //TODO: handle non-strings

    if (String.class.equals(expression.getType())) {
      if (currentPath.endsWith(",")) {
        currentPath = currentPath.substring(0, currentPath.length() - 1);
      }

      WhereParams whereParams = new WhereParams();
      whereParams.setFieldName(currentPath);
      whereParams.setValue(expression.getConstant());
      whereParams.setAnalyzed(true);
      whereParams.setAllowWildcards(false);
      whereParams.setNestedPath(false);
      luceneQuery.whereEquals(whereParams);
    }
  }

  //TODO: private void VisitMethodCall(MethodCallExpression expression, bool negated = false)



  private void visitOperation(Operation<?> expression) {
    if (expression.getOperator().getId().startsWith(LinqOps.Query.QUERY_OPERATORS_PREFIX)) {
      visitQueryableMethodCall(expression);
      //TODO: finish me
    } else {
      throw new IllegalArgumentException("Expression is not supported:" + expression);
    }
  }

  @SuppressWarnings("unchecked")
  private void visitQueryableMethodCall(Operation< ? > expression) {
    //TODO finish me

    //TODO of type
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
      //TODO: select
    } else if (operatorId.equals(LinqOps.Query.SKIP.getId())) {
      visitExpression(expression.getArg(0));
      visitSkip((Constant<Integer>) expression.getArg(1));
    } else if (operatorId.equals(LinqOps.Query.TAKE.getId())) {
      visitExpression(expression.getArg(0));
      visitTake((Constant<Integer>) expression.getArg(1));
    } else if (operatorId.equals(LinqOps.Query.FIRST_OR_DEFAULT.getId())) {
      visitExpression(expression.getArg(0));
      visitFirstOrDefault();
    } else if (operatorId.equals(LinqOps.Query.FIRST.getId())) {
      visitExpression(expression.getArg(0));
      visitFirst();
    } else if (operatorId.equals(LinqOps.Query.SINGLE.getId())) {
      visitExpression(expression.getArg(0));
      visitSingle();
    } else if (operatorId.equals(LinqOps.Query.SINGLE_OR_DEFAULT.getId())) {
      visitExpression(expression.getArg(0));
      visitSingleOrDefault();
    } else if (operatorId.equals(LinqOps.Query.COUNT.getId())) {
      visitExpression(expression.getArg(0));
      visitCount();
      //TODO: all, any, count, long count, distinct, order by, then by, then by descinding, order by descending
    } else if (operatorId.equals(LinqOps.Query.ORDER_BY.getId())) {
      visitExpression(expression.getArg(0));
      Expression< ? > orderSpecExpression = expression.getArg(1);
      if (orderSpecExpression instanceof Constant) {
        Object constant = ((Constant<?>) orderSpecExpression).getConstant();
        visitOrderBy((OrderSpecifier<?>[])constant);
      } else {
        throw new IllegalStateException("Constant expected in: " + expression);
      }
    } else if (operatorId.equals(LinqOps.Query.SEARCH.getId())) {
      visitSearch(expression);
    } else if (operatorId.equals(LinqOps.Query.ANY.getId())) {
      visitAny(expression);
    } else {
      throw new IllegalStateException("Unhandled expression: " + expression);
    }
    // TODO Auto-generated method stub
  }

  public void visitSearch(Operation<?> searchExpression) {
    List<Operation<?>> expressions = new ArrayList<>();

    Operation<?> search = searchExpression;
    Expression<?> target = searchExpression.getArg(0);
    Reference<Object> valueRef = new Reference<>();

    while (true) {
      expressions.add(search);

      if (!LinqPathProvider.getValueFromExpressionWithoutConversion(search.getArg(4), valueRef)) {
        throw new IllegalStateException("Could not extract value from " + searchExpression);
      }
      SearchOptions queryOptions = (SearchOptions) valueRef.value;
      if (queryOptions != SearchOptions.GUESS) { //TODO: in C# search option is flags
        break;
      }
      Expression< ? > maybeInnerOperation = search.getArg(0);
      if (maybeInnerOperation instanceof Operation) {
        Operation<?> innerOperation = (Operation<?>) maybeInnerOperation;
        if (LinqOps.Query.SEARCH.getId().equals(innerOperation.getOperator().getId())) {
          target = search.getArg(0);
          continue;
        }
      }
      break;
    }

    visitExpression(target);
    if (expressions.size() > 1) {
      luceneQuery.openSubclause();
    }

    for (Operation<?> expression : Lists.reverse(expressions)) {
      ExpressionInfo expressionInfo = getMember(expression.getArg(1));
      if (LinqPathProvider.getValueFromExpressionWithoutConversion(expression.getArg(2), valueRef) == false) {
        throw new IllegalArgumentException("Could not extract value from " + expression);
      }
      String searchTerms = (String) valueRef.value;
      if (LinqPathProvider.getValueFromExpressionWithoutConversion(expression.getArg(3), valueRef) == false) {
        throw new IllegalArgumentException("Could not extract value from " + expression);
      }
      Double boost = (Double) valueRef.value;
      if (LinqPathProvider.getValueFromExpressionWithoutConversion(expression.getArg(4), valueRef) == false) {
        throw new IllegalArgumentException("Could not extract value from " + expression);
      }
      SearchOptions options = (SearchOptions) valueRef.value;
      if (chanedWhere && options == SearchOptions.AND) { //TODO: has flag
        luceneQuery.andAlso();
      }
      if (options == SearchOptions.NOT) {
        luceneQuery.negateNext();
      }
      if (LinqPathProvider.getValueFromExpressionWithoutConversion(expression.getArg(5), valueRef) == false) {
        throw new IllegalArgumentException("Could not extract value from " + expression);
      }
      EscapeQueryOptions queryOptions = (EscapeQueryOptions) valueRef.value;
      luceneQuery.search(expressionInfo.getPath(), searchTerms, queryOptions);
      luceneQuery.boost(boost);

      if (options == SearchOptions.AND) {
        chanedWhere = true;
      }
    }


    if (expressions.size() > 1) {
      luceneQuery.closeSubclause();
    }

    if (LinqPathProvider.getValueFromExpressionWithoutConversion(searchExpression.getArg(4), valueRef) == false) {
      throw new IllegalArgumentException("Could not extract value from " + searchExpression);
    }
    SearchOptions options = (SearchOptions) valueRef.value;
    if (options == SearchOptions.GUESS) {
      chanedWhere = true;
    }


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

  //TODO: private void VisitSelect(Expression operand)

  private void visitSkip(Constant<Integer> constantExpression) {
    //Don't have to worry about the cast failing, the Skip() extension method only takes an int
    luceneQuery.skip((int) constantExpression.getConstant());
  }

  private void visitTake(Constant<Integer> constantExpression) {
    //Don't have to worry about the cast failing, the Take() extension method only takes an int
    luceneQuery.take((int) constantExpression.getConstant());
  }

  //TODO: visit all

  private void visitAny(Operation<?> exp) {
    ExpressionInfo memberInfo = getMember(exp.getArg(0));

    String oldPath = currentPath;
    currentPath = memberInfo.getPath() + ",";
    visitExpression(exp.getArg(1));
    currentPath = oldPath;
  }


  private void visitCount() {
    luceneQuery.take(0);
    queryType = SpecialQueryType.COUNT;
  }

  private void visitLongCount() {
    luceneQuery.take(0);
    queryType = SpecialQueryType.LONG_COUNT;
  }

  private void visitSingle() {
    luceneQuery.take(2);
    queryType = SpecialQueryType.SINGLE;
  }

  private void visitSingleOrDefault() {
    luceneQuery.take(2);
    queryType = SpecialQueryType.SINGLE_OR_DEFAULT;
  }

  private void visitFirst() {
    luceneQuery.take(1);
    queryType = SpecialQueryType.FIRST;
  }

  private void visitFirstOrDefault() {
    luceneQuery.take(1);
    queryType = SpecialQueryType.FIRST_OR_DEFAULT;
  }

  private String getFieldNameForRangeQuery(ExpressionInfo expression, Object value) {
    Field identityProperty = luceneQuery.getDocumentConvention().getIdentityProperty(clazz);
    if (identityProperty != null && identityProperty.getName().equals(expression.getPath())) {
      return Constants.DOCUMENT_ID_FIELD_NAME;
    }
    if (luceneQuery.getDocumentConvention().usesRangeType(value) && !expression.getPath().endsWith("_Range")) {
      return expression.getPath() + "_Range";
    }
    return expression.getPath();
  }

  @SuppressWarnings("unchecked")
  public IDocumentQuery<T> getLuceneQueryFor(Expression<?> expression) {
    IDocumentQuery<T> q = queryGenerator.luceneQuery(clazz, indexName, isMapReduce);
    luceneQuery = (IAbstractDocumentQuery<T>) q;

    visitExpression(expression);
    if (customizeQuery != null) {
      customizeQuery.customize(new DocumentQueryCustomiation((DocumentQuery< ? >) luceneQuery));
    }
    return q.selectFields(clazz, fieldsToFetch.toArray(new String[0]));
  }

  @SuppressWarnings("unchecked")
  public Object execute(Expression<?> expression) {

    expression = tranformAny(expression);

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

  private Expression< ? > tranformAny(Expression< ? > expression) {
    CollectionAnyVisitor visitor = new CollectionAnyVisitor();
    StackBasedContext context = new StackBasedContext();
    Expression< ? > afterAny = expression.accept(visitor, context);
    return afterAny;
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
