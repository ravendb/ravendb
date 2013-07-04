package raven.linq.dsl;

import com.mysema.query.types.Ops;
import com.mysema.query.types.Templates;

public class LinqQueryTemplates extends Templates {

  public static final LinqQueryTemplates DEFAULT = new LinqQueryTemplates();

  public LinqQueryTemplates() {

    add(Ops.EQ, "{0} == {1}", 18);

    add(Ops.COL_SIZE, "{0}.Length");

    add(Ops.STARTS_WITH, "{0}.StartsWith({1})");
    add(Ops.ENDS_WITH, "{0}.EndsWith({1})");

    add(LinqOps.SUM, "{0}.Sum({1})");
    add(LinqOps.LAMBDA, "{0} => {1}");

    add(LinqOps.Fluent.GROUP_BY, "{0}.GroupBy({1})");
    add(LinqOps.Fluent.ORDER_BY, "{0}.OrderBy({1})");
    add(LinqOps.Fluent.ORDER_BY_DESC, "{0}.OrderByDescending({1})");
    add(LinqOps.Fluent.SELECT, "{0}.Select({1})");
    add(LinqOps.Fluent.SELECT_MANY, "{0}.SelectMany({1})");
    add(LinqOps.Fluent.WHERE, "{0}.Where({1})");


    //TODO: work on another templates + create super class for general .net templates
  }


}
