package raven.linq.dsl.visitors;

import com.mysema.query.support.Context;

public class LocationAwareContext extends Context {
  public boolean inLeftLambda;
  public int innerExprNum = 1;
}
