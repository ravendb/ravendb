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
  public static class Fluent {
    public static final Operator<LinqExpressionMixin> GROUP_BY = new OperatorImpl<>("FLUENT_GROUP_BY");
    public static final Operator<LinqExpressionMixin> ORDER_BY = new OperatorImpl<>("FLUENT_ORDER_BY");
    public static final Operator<LinqExpressionMixin> ORDER_BY_DESC = new OperatorImpl<>("FLUENT_ORDER_BY_DESC");
    public static final Operator<LinqExpressionMixin> SELECT = new OperatorImpl<>("FLUENT_SELECT");
    public static final Operator<LinqExpressionMixin> SELECT_MANY = new OperatorImpl<>("FLUENT_SELECT_MANY");
    public static final Operator<LinqExpressionMixin> WHERE = new OperatorImpl<>("FLUENT_WHERE");
  }

}
