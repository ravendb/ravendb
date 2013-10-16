package net.ravendb.client.linq;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ravendb.abstractions.LinqOps;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.EscapeQueryOptions;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.SearchOptions;
import net.ravendb.client.SearchOptionsSet;
import net.ravendb.client.WhereParams;
import net.ravendb.client.document.DocumentQuery;
import net.ravendb.client.document.DocumentQueryCustomization;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.IAbstractDocumentQuery;
import net.ravendb.client.linq.LinqPathProvider.Result;


import com.google.common.collect.Lists;
import com.mysema.codegen.StringUtils;
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

  private boolean insideSelect = false;
  private final boolean isMapReduce;


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
      }
      visitConstant((Constant<?>) expression, true);

    } else if (expression instanceof Path) {
      visitMemberAccess((Path<?>)expression, true);
    } else {
      throw new IllegalArgumentException("Expression is not supported:" + expression);
    }
  }

  private void visitMemberAccess(Path< ? > expression, boolean boolValue) {
    if (expression.getType().equals(Boolean.class)) {
      ExpressionInfo memberInfo = getMember(expression);

      WhereParams whereParams = new WhereParams();
      whereParams.setFieldName(memberInfo.getPath());
      whereParams.setValue(boolValue);
      whereParams.setAnalyzed(true);
      whereParams.setAllowWildcards(false);
      luceneQuery.whereEquals(whereParams);
    } else {
      throw new IllegalStateException("Path type is not supported: " + expression + " " + expression.getType());
    }
  }

  private void visitBooleanOperation(Operation<Boolean> expression) {
    if (expression.getOperator().equals(Ops.OR)) {
      visitOrElse(expression);
    } else if (expression.getOperator().equals(Ops.AND)) {
      visitAndAlso(expression);
    } else if (expression.getOperator().equals(Ops.NE)) {
      visitNotEquals(expression);
    } else if (expression.getOperator().equals(Ops.EQ)) {
      visitEquals(expression);
    } else if (expression.getOperator().equals(Ops.EQ_IGNORE_CASE)) {
      visitEqualsIgnoreCase(expression);
    } else if (expression.getOperator().equals(LinqOps.Ops.EQ_NOT_IGNORE_CASE)) {
      visitEqualsNotIgnoreCase(expression);
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
        } else if (subOp1.getOperator().equals(Ops.STRING_IS_EMPTY)) {
          visitStringEmpty(subOp1.getArg(0), true);
          return;
        }
      } else if (subExpr1 instanceof Path) {
        visitMemberAccess((Path< ? >) subExpr1, false);
        return;
      }

      luceneQuery.openSubclause();
      luceneQuery.where("*:*");
      luceneQuery.andAlso();
      luceneQuery.negateNext();
      visitExpression(expression.getArg(0));
      luceneQuery.closeSubclause();
    } else if (expression.getOperator().equals(Ops.COL_IS_EMPTY)) {
      visitCollectionEmpty(expression.getArg(0), true);
    } else if (expression.getOperator().equals(Ops.IN)) {
      if (expression.getArg(0) instanceof Constant && expression.getArg(1) instanceof Path) {
        visitContains(expression);
      } else if (expression.getArg(0) instanceof Path && expression.getArg(1) instanceof Constant) {
        visitIn(expression);
      } else {
        throw new IllegalStateException("Unable to handle in/contains expression: " + expression);
      }
    } else if (expression.getOperator().equals(Ops.CONTAINS_KEY)) {
      visitContainsKey(expression);
    } else if (expression.getOperator().equals(Ops.CONTAINS_VALUE)) {
      visitContainsValue(expression);
    } else if (expression.getOperator().equals(Ops.IS_NULL)) {
      visitIsNull(expression);
    } else if (expression.getOperator().equals(Ops.STRING_IS_EMPTY)) {
      visitStringEmpty(expression.getArg(0), false);
    } else if (expression.getOperator().equals(Ops.STRING_CONTAINS)) {
      throw new IllegalStateException("Contains is not supported, doing a substring match over a text field is a" +
        " very slow operation, and is not allowed using the Linq API. The recommended method is to use full text search (mark the field as Analyzed and use the search() method to query it.");
    } else if (expression.getOperator().equals(Ops.STARTS_WITH)) {
      visitStartsWith(expression);
    } else if (expression.getOperator().equals(Ops.ENDS_WITH)) {
      visitEndsWith(expression);
    } else if (expression.getOperator().equals(LinqOps.Query.ANY)) {
      visitAny(expression);
    } else if (expression.getOperator().equals(LinqOps.Query.ANY_RESULT)) {
      visitAny();
    } else {
      throw new IllegalArgumentException("Expression is not supported: " + expression.getOperator());
    }
  }


  @SuppressWarnings("unchecked")
  private void visitIn(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    Object objects = getValueFromExpression(expression.getArg(1), getMemberType(memberInfo));
    luceneQuery.whereIn(memberInfo.getPath(), (Collection<Object>)objects);
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

  public void visitStringEmpty(Expression<?> expression, boolean isNegated) {

    if (isNegated) {
      luceneQuery.openSubclause();
      luceneQuery.where("*:*");
      luceneQuery.andAlso();
      luceneQuery.negateNext();
    }
    ExpressionInfo memberInfo = getMember(expression);
    luceneQuery.openSubclause();
    luceneQuery.whereEquals(memberInfo.getPath(), Constants.NULL_VALUE, false);
    luceneQuery.orElse();
    luceneQuery.whereEquals(memberInfo.getPath(), Constants.EMPTY_STRING, false);
    luceneQuery.closeSubclause();

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

  private void visitEqualsIgnoreCase(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue(getValueFromExpression(expression.getArg(1), getMemberType(memberInfo)));
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(false);
    luceneQuery.whereEquals(whereParams);
  }

  private void visitEqualsNotIgnoreCase(Operation<Boolean> expression) {
    ExpressionInfo memberInfo = getMember(expression.getArg(0));
    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue(getValueFromExpression(expression.getArg(1), getMemberType(memberInfo)));
    whereParams.setAnalyzed(false);
    whereParams.setAllowWildcards(false);
    luceneQuery.whereEquals(whereParams);
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

    if (constantExpression != null && Boolean.FALSE.equals(constantExpression.getConstant())
      && !(expression.getArg(0) instanceof Path)) {
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
    if (!(arg instanceof Path<?>)) {
      return false;
    }
    return true;
  }

  private void visitNotEquals(Operation<Boolean> expression) {
    if (!isMemberAccessForQuerySource(expression.getArg(0)) && isMemberAccessForQuerySource(expression.getArg(1))) {
      visitEquals((BooleanOperation) Expressions.booleanOperation(Ops.NE, expression.getArg(1), expression.getArg(0)));
      return ;
    }

    ExpressionInfo memberInfo = getMember(expression.getArg(0));

    luceneQuery.openSubclause();
    luceneQuery.negateNext();

    WhereParams whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue(getValueFromExpression(expression.getArg(1), getMemberType(memberInfo)));
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(false);
    luceneQuery.whereEquals(whereParams);
    luceneQuery.andAlso();
    whereParams = new WhereParams();
    whereParams.setFieldName(memberInfo.getPath());
    whereParams.setValue("*");
    whereParams.setAnalyzed(true);
    whereParams.setAllowWildcards(true);
    luceneQuery.whereEquals(whereParams);
    luceneQuery.closeSubclause();
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

    String propertyName = indexName == null  || indexName.toLowerCase().startsWith("dynamic/")
      ? queryGenerator.getConventions().getFindPropertyNameForDynamicIndex().find(clazz, indexName, currentPath, result.getPath())
        : queryGenerator.getConventions().getFindPropertyNameForIndex().find(clazz, indexName, currentPath, result.getPath());

      ExpressionInfo expressionInfo = new ExpressionInfo(propertyName, result.getMemberType(), result.isNestedPath());
      expressionInfo.setMaybeProperty(result.getMaybeProperty());
      return expressionInfo;
  }

  private static Param<?> getParameterExpressionIncludingConvertions(Expression<?> expression) {
    if (expression instanceof ParamExpression){
      return (Param< ? >) expression;
    } else if (expression instanceof Path) {
      Path<?> path = (Path<?>) expression;
      if (path.getMetadata().getParent() == null) {
        return new Param<>(path.getType(), path.getMetadata().getName());
      }
    }

    return null;
  }

  private void visitStartsWith(Operation<Boolean> operation) {
    Expression< ? > expression = operation.getArg(0);
    ExpressionInfo memberInfo = getMember(expression);
    luceneQuery.whereStartsWith(memberInfo.getPath(), getValueFromExpression(operation.getArg(1), getMemberType(memberInfo)));
  }

  private void visitEndsWith(Operation<Boolean> operation) {
    Expression< ? > expression = operation.getArg(0);
    ExpressionInfo memberInfo = getMember(expression);
    luceneQuery.whereEndsWith(memberInfo.getPath(), getValueFromExpression(operation.getArg(1), getMemberType(memberInfo)));
  }

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

  private void visitConstant(Constant< ? > expression, boolean boolValue) {
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
    } else {
      throw new IllegalStateException("Unable to handle constant:" + expression.getConstant());
    }
  }

  private void visitOperation(Operation<?> expression) {
    if (expression.getOperator().getId().startsWith(LinqOps.Query.QUERY_OPERATORS_PREFIX)) {
      visitQueryableMethodCall(expression);
    } else {
      throw new IllegalArgumentException("Expression is not supported:" + expression);
    }
  }

  @SuppressWarnings("unchecked")
  private void visitQueryableMethodCall(Operation< ? > expression) {
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
    } else if (operatorId.equals(LinqOps.Query.SKIP.getId())) {
      visitExpression(expression.getArg(0));
      visitSkip((Constant<Integer>) expression.getArg(1));
    } else if (operatorId.equals(LinqOps.Query.TAKE.getId())) {
      visitExpression(expression.getArg(0));
      visitTake((Constant<Integer>) expression.getArg(1));
    } else if (operatorId.equals(LinqOps.Query.DISTINCT.getId())) {
      luceneQuery.distinct();
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
    } else if (operatorId.equals(LinqOps.Query.LONG_COUNT.getId())) {
      visitExpression(expression.getArg(0));
      visitLongCount();
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
    } else if (operatorId.equals(LinqOps.Query.INTERSECT.getId())) {
      visitExpression(expression.getArg(0));
      luceneQuery.intersect();
      chanedWhere = false;
    } else if (operatorId.equals(LinqOps.Query.SELECT.getId())) {

      Class<?> rootType = extractRootTypeForSelect(expression.getArg(1));
      if (rootType != null) {
        luceneQuery.addRootType((Class<T>) rootType);
      }

      visitExpression(expression.getArg(0));
      visitSelect(expression);
    } else {
      throw new IllegalStateException("Unhandled expression: " + expression);
    }
  }

  private Class<?> extractRootTypeForSelect(Expression<?> expression) {
    if (expression instanceof Path) {
      Path<?> path = (Path<?>) expression;
      return path.getType();
    } else if (expression instanceof Constant) {
      Constant<?> constant = (Constant<?>) expression;
      return (Class< ? >) constant.getConstant();
    } else {
      throw new IllegalStateException("Don't know how to fetch root type for select: " + expression);
    }
  }

  private void visitSelect(Operation< ? > expression) {
    Expression< ? > projectionExpr = expression.getArg(1);
    if (projectionExpr instanceof Path) {
      // projection via x.someProperty
      Path<?> path = (Path<?>)projectionExpr;
      addToFieldsToFetch(getSelectPath(path), getSelectPath(path));
      if (insideSelect == false) {
        Set<RenamedField> toDelete = new HashSet<>();
        for (RenamedField renamedField : fieldsToRename) {
          if (renamedField.getOriginalField().equals(StringUtils.capitalize(path.getMetadata().getName()))) {
            toDelete.add(renamedField);
          }
          fieldsToRename.removeAll(toDelete);
        }

        RenamedField renamedField = new RenamedField();
        renamedField.setNewField(null);
        renamedField.setOriginalField(StringUtils.capitalize(path.getMetadata().getName()));
        fieldsToRename.add(renamedField);
      }

    } else if (projectionExpr instanceof Constant) {
      // projection via Class<TProjection>
      Constant< ? > projectionClassConst = (Constant< ? >) expression.getArg(1);
      Class<?> projectionClass = (Class< ? >) projectionClassConst.getConstant();

      int astArgCount = expression.getArgs().size();

      String[] fields = null;
      String[] projections = null;

      switch (astArgCount) {
        case 2:
          // extract mappings using reflection
          List<String> fieldsList = new ArrayList<>();

          try {
            for (PropertyDescriptor propertyDescriptor : Introspector.getBeanInfo(projectionClass).getPropertyDescriptors()) {
              if (propertyDescriptor.getWriteMethod() != null && propertyDescriptor.getReadMethod() != null) {
                fieldsList.add(StringUtils.capitalize(propertyDescriptor.getName()));
              }
            }
          } catch (IntrospectionException e) {
            throw new RuntimeException(e);
          }
          fields = fieldsList.toArray(new String[0]);
          projections = fieldsList.toArray(new String[0]);

          break;
        case 4:
          // we have already extracted projections
          fields = (String[]) ((Constant<?>)expression.getArg(2)).getConstant();
          projections = (String[]) ((Constant<?>)expression.getArg(3)).getConstant();
          break;
        default:
          throw new IllegalStateException("Unexpected number of nodes in select: " + expression);
      }

      for (int i = 0; i < fields.length; i++) {
        addToFieldsToFetch(fields[i], projections[i]);
      }


    } else {
      throw new IllegalStateException("Unhandled select expression: " + expression);
    }
  }

  private void addToFieldsToFetch(String docField, String renamedField) {
    Field identityProperty = luceneQuery.getDocumentConvention().getIdentityProperty(clazz);
    if (identityProperty != null && identityProperty.getName().equals(docField)) {
      fieldsToFetch.add(Constants.DOCUMENT_ID_FIELD_NAME);
      if (!identityProperty.getName().equals(renamedField)) {
        docField = Constants.DOCUMENT_ID_FIELD_NAME;
      }
    } else {
      fieldsToFetch.add(docField);
    }
    if (!docField.equals(renamedField)) {
      if (identityProperty == null) {
        String idPropName = luceneQuery.getDocumentConvention().getFindIdentityPropertyNameFromEntityName().find(luceneQuery.getDocumentConvention().getTypeTagName(clazz));
        if (docField.equals(idPropName)) {
          RenamedField renamedField2 = new RenamedField();
          renamedField2.setNewField(renamedField);
          renamedField2.setOriginalField(Constants.DOCUMENT_ID_FIELD_NAME);
          fieldsToRename.add(renamedField2);
        }
      }
      RenamedField renamedField3 = new RenamedField();
      renamedField3.setNewField(renamedField);
      renamedField3.setOriginalField(docField);
      fieldsToRename.add(renamedField3);
    }
  }

  private String getSelectPath(Path<?> expression) {
    ExpressionInfo expressionInfo = getMember(expression);
    return expressionInfo.getPath();
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
      SearchOptionsSet queryOptions = (SearchOptionsSet) valueRef.value;
      if (!queryOptions.contains(SearchOptions.GUESS)) {
        break;
      }
      Expression< ? > maybeInnerOperation = search.getArg(0);
      if (maybeInnerOperation instanceof Operation) {
        search = (Operation< ? >) maybeInnerOperation;
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
      SearchOptionsSet options = (SearchOptionsSet) valueRef.value;
      if (chanedWhere && options.contains(SearchOptions.AND)) {
        luceneQuery.andAlso();
      }
      if (options.contains(SearchOptions.NOT)) {
        luceneQuery.negateNext();
      }
      if (LinqPathProvider.getValueFromExpressionWithoutConversion(expression.getArg(5), valueRef) == false) {
        throw new IllegalArgumentException("Could not extract value from " + expression);
      }
      EscapeQueryOptions queryOptions = (EscapeQueryOptions) valueRef.value;
      luceneQuery.search(expressionInfo.getPath(), searchTerms, queryOptions);
      luceneQuery.boost(boost);

      if (options.contains(SearchOptions.AND)) {
        chanedWhere = true;
      }
    }


    if (expressions.size() > 1) {
      luceneQuery.closeSubclause();
    }

    if (LinqPathProvider.getValueFromExpressionWithoutConversion(searchExpression.getArg(4), valueRef) == false) {
      throw new IllegalArgumentException("Could not extract value from " + searchExpression);
    }
    SearchOptionsSet options = (SearchOptionsSet) valueRef.value;
    if (options.contains(SearchOptions.GUESS)) {
      chanedWhere = true;
    }


  }

  private void visitOrderBy(OrderSpecifier< ? >[] orderSpecs) {
    for (OrderSpecifier<?> orderSpec : orderSpecs) {
      ExpressionInfo result = getMemberDirect(orderSpec.getTarget());
      Class<?> fieldType = result.getClazz();
      String fieldName = result.getPath();

      if (result.getMaybeProperty() != null && queryGenerator.getConventions().getFindIdentityProperty().find(result.getMaybeProperty())) {
        fieldName = Constants.DOCUMENT_ID_FIELD_NAME;
        fieldType = String.class;
      }

      if (queryGenerator.getConventions().usesRangeType(fieldType)) {
        fieldName += "_Range";
      }
      luceneQuery.addOrder(fieldName, orderSpec.getOrder() == Order.DESC, fieldType);

    }
  }

  private void visitSkip(Constant<Integer> constantExpression) {
    //Don't have to worry about the cast failing, the Skip() extension method only takes an int
    luceneQuery.skip(constantExpression.getConstant());
  }

  private void visitTake(Constant<Integer> constantExpression) {
    //Don't have to worry about the cast failing, the Take() extension method only takes an int
    luceneQuery.take(constantExpression.getConstant());
  }

  private void visitAny(Operation<?> exp) {
    ExpressionInfo memberInfo = getMember(exp.getArg(0));

    String oldPath = currentPath;
    currentPath = memberInfo.getPath() + ",";
    visitExpression(exp.getArg(1));
    currentPath = oldPath;
  }

  private void visitAny() {
    luceneQuery.take(1);
    queryType = SpecialQueryType.ANY;
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

    q.setResultTransformer(resultsTransformer);
    visitExpression(expression);
    if (customizeQuery != null) {
      customizeQuery.customize(new DocumentQueryCustomization((DocumentQuery< ? >) luceneQuery));
    }
    return q.selectFields(clazz, fieldsToFetch.toArray(new String[0]));
  }

  @SuppressWarnings("unchecked")
  public Object execute(Expression<?> expression) {

    chanedWhere = false;

    luceneQuery = (IAbstractDocumentQuery<T>) getLuceneQueryFor(expression);
    if (newExpressionType.equals(clazz)) {
      return executeQuery(clazz);
    } else {
      throw new IllegalStateException("Don't know how to handle expression:" + expression);
    }
  }

  @SuppressWarnings("unchecked")
  private <TProjection> Object executeQuery(Class<TProjection> projectionClass) {
    List<String> renamedFields = new ArrayList<>();
    outer:
      for (String field :fieldsToFetch) {
        for (RenamedField renamedField : fieldsToRename) {
          if (renamedField.getOriginalField().equals(field)) {
            renamedFields.add(renamedField.getNewField() != null ? renamedField.getNewField() : field);
            continue outer;
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

  public void renameResults(QueryResult queryResult) {

    for (int index = 0; index < queryResult.getResults().size(); index++) {
      RavenJObject result = queryResult.getResults().get(index);
      RavenJObject safeToModify = result.createSnapshot();
      boolean changed = false;
      Map<String, RavenJToken> values = new HashMap<>();

      Set<String> renamedFieldSet = new HashSet<>();
      for (RenamedField field : fieldsToRename) {
        renamedFieldSet.add(field.getOriginalField());
      }

      for(String renamedField : renamedFieldSet) {
        Reference<RavenJToken> valueRef = new Reference<>();
        if (safeToModify.tryGetValue(renamedField, valueRef) == false) {
          continue;
        }
        values.put(renamedField, valueRef.value);
        safeToModify.remove(renamedField);
      }
      for (RenamedField rename : fieldsToRename) {
        if (!values.containsKey(rename.getOriginalField())) {
          continue;
        }
        RavenJToken val = values.get(rename.getOriginalField());
        changed = true;
        RavenJObject ravenJObject = (RavenJObject) ((val instanceof RavenJObject) ? val : null);
        if (rename.getNewField() == null && ravenJObject != null) {
          safeToModify = ravenJObject;
        } else if (rename.getNewField() != null) {
          safeToModify.set(rename.getNewField(), val);
        } else {
          safeToModify.set(rename.getOriginalField(), val);
        }
      }

      if (!changed) {
        continue;
      }
      safeToModify.ensureCannotBeChangeAndEnableShapshotting();
      queryResult.getResults().set(index, safeToModify);

    }
  }

  private <TProjection> Object getQueryResult(IDocumentQuery<TProjection> finalQuery) {
    List<TProjection> list = null;
    switch (queryType)
    {
      case FIRST:
        return finalQuery.first();
      case FIRST_OR_DEFAULT:
        return finalQuery.firstOrDefault();
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
      case ANY:
        return finalQuery.any();
      case COUNT:
        return finalQuery.getQueryResult().getTotalResults();
      case LONG_COUNT:
        return (long)finalQuery.getQueryResult().getTotalResults();
      default:
        return finalQuery;
    }
  }


}
