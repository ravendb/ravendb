package raven.client.document;

import raven.client.linq.LinqPathProvider;

import com.mysema.query.types.Expression;

public interface CustomQueryExpressionTranslator {

  public boolean canTransform(Expression<?> expression);

  public LinqPathProvider.Result translate(Expression<?> expression);

}
