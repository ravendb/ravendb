package raven.linq.dsl;

import java.lang.reflect.InvocationTargetException;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.expr.NumberExpression;
import com.mysema.query.types.expr.NumberOperation;
import com.mysema.query.types.path.SimplePath;


public class Grouping<TKey extends Path<?>> {
  private static final String DEFAULT_GROUP_ALIAS = "group";
  private String groupAlias;
  private Class<TKey> keyClass;
  private LambdaInferer lambdaInferer;

  public TKey key;

  private Grouping(Class<TKey> keyClass, String alias) {
    super();
    this.keyClass = keyClass;
    this.groupAlias = alias;
    createKey();
    lambdaInferer = LambdaInferer.DEFAULT;
  }


  @SuppressWarnings("rawtypes")
  private void createKey() {
    SimplePath<Grouping> root = Expressions.path(Grouping.class, groupAlias);
    SimplePath<?> pathWithKey = Expressions.path(keyClass, root, "key");
    try {
      key = keyClass.getConstructor(PathMetadata.class).newInstance(pathWithKey.getMetadata());
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Can't create key!", e);
    }
  }


  public static <TKey extends Path<?>, TElement>  Grouping<TKey> create(Class<TKey> keyClass) {
    return create(keyClass, DEFAULT_GROUP_ALIAS);
  }

  public static <TKey extends Path<?>, TElement>  Grouping<TKey> create(Class<TKey> keyClass, String groupAlias) {
    return new Grouping<>(keyClass, groupAlias);
  }


  public <S extends Number & Comparable<S>> NumberExpression<S> sum(Path<S> selector) {
    return NumberOperation.create(selector.getType(), LinqOps.SUM, Expressions.path(keyClass, groupAlias), lambdaInferer.inferLambdas(selector));
  }


}
