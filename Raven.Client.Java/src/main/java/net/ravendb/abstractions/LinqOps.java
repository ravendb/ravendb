package net.ravendb.abstractions;

import com.mysema.query.types.Operator;
import com.mysema.query.types.OperatorImpl;

/**
 * Linq Operations
 */
public class LinqOps {

  public static class Query {
    public static final Operator<Object> WHERE = new OperatorImpl<>("QUERY_WHERE");
    public static final Operator<Object> ORDER_BY = new OperatorImpl<>("QUERY_ORDER_BY");
    public static final Operator<Object> SEARCH = new OperatorImpl<>("QUERY_SEARCH");
    public static final Operator<Object> ORDER_BY_SCORE = new OperatorImpl<>("QUERY_ORDER_BY_SCORE");
    public static final Operator<Object> INTERSECT = new OperatorImpl<>("QUERY_INTERSECT");

    public static final Operator<Object> FIRST_OR_DEFAULT = new OperatorImpl<>("QUERY_FIRST_OR_DEFAULT");
    public static final Operator<Object> FIRST = new OperatorImpl<>("QUERY_FIRST");
    public static final Operator<Object> COUNT = new OperatorImpl<>("QUERY_COUNT");
    public static final Operator<Object> LONG_COUNT = new OperatorImpl<>("QUERY_LONG_COUNT");
    public static final Operator<Object> SINGLE = new OperatorImpl<>("QUERY_SINGLE");
    public static final Operator<Object> SINGLE_OR_DEFAULT = new OperatorImpl<>("QUERY_SINGLE_OR_DEFAULT");
    public static final Operator<Object> SKIP = new OperatorImpl<>("QUERY_SKIP");
    public static final Operator<Object> TAKE = new OperatorImpl<>("QUERY_TAKE");
    public static final Operator<Object> DISTINCT = new OperatorImpl<>("QUERY_DISTINCT");
    public static final Operator<Object> SELECT = new OperatorImpl<>("QUERY_SELECT");

    public static final Operator<Object> ANY = new OperatorImpl<>("QUERY_ANY");

    public static final Operator<Object> ANY_RESULT = new OperatorImpl<>("QUERY_ANY_RESULT");

    public static final String QUERY_OPERATORS_PREFIX = "QUERY_";
  }

  public static final class Ops {

    public static final Operator<Boolean> EQ_NOT_IGNORE_CASE = new OperatorImpl<>("EQ_NOT_IGNORE_CASE");

  }


}
