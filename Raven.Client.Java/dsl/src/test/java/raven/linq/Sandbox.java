package raven.linq;

import raven.linq.dsl.Grouping;
import raven.samples.PersonResult;
import raven.samples.QPersonResult;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.path.SimplePath;

public class Sandbox {

  public static void main(String[] args) {
    SimplePath<Grouping> root = Expressions.path(Grouping.class, "group");
    SimplePath<PersonResult> personResult = Expressions.path(PersonResult.class, root, "key");

    QPersonResult result = new QPersonResult(personResult);
    System.out.println(personResult);
    System.out.println(result);




  }

}
