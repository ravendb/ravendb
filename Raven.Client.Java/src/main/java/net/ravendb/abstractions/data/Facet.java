package net.ravendb.abstractions.data;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.indexing.NumberUtil;
import net.ravendb.abstractions.util.NetDateFormat;
import net.ravendb.abstractions.util.RavenQuery;

import org.apache.commons.lang.StringUtils;


import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Path;
import com.mysema.query.types.PredicateOperation;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.BooleanOperation;

public class Facet {
  private String displayName;
  private FacetMode mode = FacetMode.DEFAULT;
  private FacetAggregationSet aggregation = new FacetAggregationSet();
  private String aggregationField;
  private String aggregationType;
  private String name;
  private List<String> ranges;
  private Integer maxResults;
  private FacetTermSortMode termSortMode;
  private Boolean includeRemainingTerms = false;

  public Facet() {
    this.ranges = new ArrayList<>();
    this.termSortMode = FacetTermSortMode.VALUE_ASC;
  }

  public FacetAggregationSet getAggregation() {
    return aggregation;
  }

  public void setAggregation(FacetAggregationSet aggregation) {
    this.aggregation = aggregation;
  }

  public String getAggregationType() {
    return aggregationType;
  }

  public void setAggregationType(String aggregationType) {
    this.aggregationType = aggregationType;
  }

  public String getAggregationField() {
    return aggregationField;
  }
  public String getDisplayName() {
    return displayName != null ? displayName : name;
  }
  public Integer getMaxResults() {
    return maxResults;
  }
  public FacetMode getMode() {
    return mode;
  }
  public String getName() {
    return name;
  }
  public List<String> getRanges() {
    return ranges;
  }
  public FacetTermSortMode getTermSortMode() {
    return termSortMode;
  }
  public Boolean getIncludeRemainingTerms() {
    return includeRemainingTerms;
  }

  public void setIncludeRemainingTerms(Boolean includeRemainingTerms) {
    this.includeRemainingTerms = includeRemainingTerms;
  }

  public void setAggregationField(String aggregationField) {
    this.aggregationField = aggregationField;
  }
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }
  public void setMaxResults(Integer maxResults) {
    this.maxResults = maxResults;
  }
  public void setMode(FacetMode mode) {
    this.mode = mode;
  }
  public void setName(String name) {
    this.name = name;
  }
  public void setRanges(List<String> ranges) {
    this.ranges = ranges;
  }
  public void setTermSortMode(FacetTermSortMode termSortMode) {
    this.termSortMode = termSortMode;
  }


  public void setRanges(BooleanExpression... exprs) {
    for (BooleanExpression expr: exprs) {
      this.ranges.add(parse(expr));
    }

    boolean shouldUseRanges = ranges.size() > 0;
    mode = shouldUseRanges ? FacetMode.RANGES : FacetMode.DEFAULT;
    if (name == null) {
      throw new IllegalStateException("Set facet name first!");
    }
    if (!name.endsWith("_Range")) {
      name += "_Range";
    }

  }

  public void setName(Path<?> path) {
    name = StringUtils.capitalize(path.getMetadata().getName());
  }

  @SuppressWarnings("unchecked")
  public static String parse(BooleanExpression expr) {
    BooleanOperation operation = (BooleanOperation) expr;
    if (operation.getArg(0) instanceof Path<?>) {
      Object subExpressionValue = parseSubExpression(operation);
      String expression = getStringRepresentation(operation.getOperator(), subExpressionValue);
      return expression;
    }

    Expression< ? > leftExpr = operation.getArg(0);
    Expression< ? > rightExpr = operation.getArg(1);

    boolean leftExprOk = (leftExpr instanceof BooleanOperation) || (leftExpr instanceof PredicateOperation);
    boolean rightExprOk = (rightExpr instanceof BooleanOperation) || (rightExpr instanceof PredicateOperation);

    if (!leftExprOk || !rightExprOk || !operation.getOperator().equals(Ops.AND)) {
      throw new IllegalArgumentException("Expression doesn't have the correct sub-expression. ");
    }

    Operation<Boolean> left = (Operation<Boolean>) leftExpr;
    Operation<Boolean> right = (Operation<Boolean>) rightExpr;

    Expression< ? > leftMemberExpr = left.getArg(0);
    Expression< ? > rightMemberExpr = right.getArg(0);

    Path<?> leftMember = (Path<?>) ((leftMemberExpr instanceof Path) ? leftMemberExpr : null);
    Path<?> rightMember = (Path<?>) ((rightMemberExpr instanceof Path) ? rightMemberExpr : null);

    boolean validOperators = (left.getOperator().equals(Ops.LT) || left.getOperator().equals(Ops.LOE)
        || left.getOperator().equals(Ops.GT) || left.getOperator().equals(Ops.GOE))
        && (right.getOperator().equals(Ops.LT) || right.getOperator().equals(Ops.LOE)
            || right.getOperator().equals(Ops.GT) || right.getOperator().equals(Ops.GOE));

    boolean validMemberNames = leftMember != null && rightMember != null
        && getFieldName(leftMember).equals(getFieldName(rightMember));

    if (validOperators && validMemberNames) {
      return getStringRepresentation(left.getOperator(), right.getOperator(), parseSubExpression(left), parseSubExpression(right));
    }
    throw new IllegalArgumentException("Members in sub-expression(s) are not the correct types (expected \"<\" and \">\")");
  }

  private static String getFieldName(Path<?> left) {
    return StringUtils.capitalize(left.getMetadata().getName());
  }

  private static Object parseSubExpression(Operation<Boolean> operation) {
    if (operation.getArg(1) instanceof Constant<?>) {
      Constant<?> right = (Constant<?>) operation.getArg(1);
      return right.getConstant();
    }

    throw new IllegalArgumentException("Unable to parse expression: " + operation);
  }
  private static String getStringRepresentation(Operator<? super Boolean> op, Object value) {
    String valueAsStr = getStringValue(value);

    if (Ops.LT.equals(op)) {
      return String.format("[NULL TO %s]", valueAsStr);
    } else if (Ops.GT.equals(op)) {
      return String.format("[%s TO NULL]", valueAsStr);
    } else if (Ops.LOE.equals(op)) {
      return String.format("[NULL TO %s}", valueAsStr);
    } else if (Ops.GOE.equals(op)) {
      return String.format("{%s TO NULL]", valueAsStr);
    }
    throw new IllegalArgumentException("Unable to parse the given operation " + op.getId() + ", into a facet range!!! ");
  }

  private static String getStringRepresentation(Operator<? super Boolean> leftOp, Operator<? super Boolean> rightOp, Object lValue, Object rValue) {
    String lValueAsStr = getStringValue(lValue);
    String rValueAsStr = getStringValue(rValue);
    if (lValueAsStr != null && rValueAsStr != null)
      return String.format("%s%s TO %s%s", calculateBraces(leftOp, true), lValueAsStr, rValueAsStr, calculateBraces(rightOp, false));
    throw new IllegalArgumentException("Unable to parse the given operation into a facet range!!! ");

  }

  private static String calculateBraces(Operator<? super Boolean> op, boolean isLeft) {
    if (op.equals(Ops.GOE) || op.equals(Ops.LOE)) {
      return isLeft ? "{" : "}";
    }
    return isLeft ? "[" : "]";
  }

  private static String getStringValue(Object value) {
    String clazzName = value.getClass().getName();

    switch (clazzName) {
    case "java.util.Date":
      NetDateFormat fdf = new NetDateFormat();
      return RavenQuery.escape(fdf.format(value));
    case "java.lang.Integer":
      return NumberUtil.numberToString(((int)value));
    case "java.lang.Long":
      return NumberUtil.numberToString((long)value);
    case "java.lang.Float":
      return NumberUtil.numberToString((float)value);
    case "java.lang.Double":
      return NumberUtil.numberToString((double)value);
    case "java.lang.String":
      return RavenQuery.escape(value.toString());
    default:
      throw new IllegalStateException("Unable to parse the given type " + value.getClass().getName() + ", into a facet range!!! ");
    }
  }

}
