package raven.linq.dsl;

import com.mysema.query.types.Operator;
import com.mysema.query.types.OperatorImpl;

/**
 * Linq Operations
 */
public class LinqOps {
  public static final Operator<Number> SUM = new OperatorImpl<Number>("ENUM_SUM");
  public static final Operator<Object> LAMBDA = new OperatorImpl<>("LAMBDA");

  @SuppressWarnings("rawtypes")
  public static class Markers {
    public static final OperatorImpl<LinqExpressionMixin> CREATE_FIELD2 = new OperatorImpl<>("CREATE_FIELD2");
    public static final OperatorImpl<LinqExpressionMixin> CREATE_FIELD4 = new OperatorImpl<>("CREATE_FIELD4");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_GENERATE2 = new OperatorImpl<>("SPATIAL_GENERATE2");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_GENERATE3 = new OperatorImpl<>("SPATIAL_GENERATE3");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_INDEX_GENERATE2 = new OperatorImpl<>("SPATIAL_INDEX_GENERATE2");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_INDEX_GENERATE3 = new OperatorImpl<>("SPATIAL_INDEX_GENERATE3");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_CLUSTERING3 = new OperatorImpl<>("SPATIAL_CLUSTERING3");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_CLUSTERING5 = new OperatorImpl<>("SPATIAL_CLUSTERING5");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_WKT_GENERATE2 = new OperatorImpl<>("SPATIAL_WKT_GENERATE2");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_WKT_GENERATE3 = new OperatorImpl<>("SPATIAL_WKT_GENERATE3");
    public static final OperatorImpl<LinqExpressionMixin> SPATIAL_WKT_GENERATE4 = new OperatorImpl<>("SPATIAL_WKT_GENERATE4");
    public static final OperatorImpl<LinqExpressionMixin> RECURSE = new OperatorImpl<>("RECURSE");
  }

  @SuppressWarnings("rawtypes")
  public static class Fluent {
    public static final Operator<LinqExpressionMixin> GROUP_BY = new OperatorImpl<>("FLUENT_GROUP_BY");
    public static final Operator<LinqExpressionMixin> ORDER_BY = new OperatorImpl<>("FLUENT_ORDER_BY");
    public static final Operator<LinqExpressionMixin> ORDER_BY_DESC = new OperatorImpl<>("FLUENT_ORDER_BY_DESC");
    public static final Operator<LinqExpressionMixin> SELECT = new OperatorImpl<>("FLUENT_SELECT");
    public static final Operator<LinqExpressionMixin> SELECT_MANY = new OperatorImpl<>("FLUENT_SELECT_MANY");
    public static final Operator<LinqExpressionMixin> SELECT_MANY_TRANSLATED = new OperatorImpl<>("FLUENT_SELECT_MANY_TRANSLATED");
    public static final Operator<LinqExpressionMixin> WHERE = new OperatorImpl<>("FLUENT_WHERE");

  }


}
