package raven.abstractions;

import com.mysema.query.types.Operator;
import com.mysema.query.types.OperatorImpl;

/**
 * Linq Operations
 */
public class LinqOps {

  public static class Query {
    public static final Operator<Object> WHERE = new OperatorImpl<>("QUERY_WHERE");
    public static final Operator<Object> ORDER_BY = new OperatorImpl<>("QUERY_ORDER_BY");
    public static final String QUERY_OPERATORS_PREFIX = "QUERY_";
  }


}
