package raven.linq;

import static org.junit.Assert.assertEquals;
import static raven.linq.dsl.IndexExpression.from;

import org.junit.Test;

import raven.linq.dsl.Grouping;
import raven.linq.dsl.IndexExpression;
import raven.linq.dsl.LinqQueryTemplates;
import raven.linq.dsl.LinqSerializer;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.samples.Company;
import raven.samples.Person;
import raven.samples.PersonResult;
import raven.samples.QCompany;
import raven.samples.QPerson;
import raven.samples.QPersonResult;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.expr.NumberExpression;
import com.mysema.query.types.path.StringPath;
import com.mysema.query.types.template.StringTemplate;


public class IndexBuildTests {

  @Test
  public void testMapReduce() {
    QPerson p = QPerson.person;
    QPersonResult pr = QPersonResult.personResult;
    IndexExpression select = from(Person.class)
        .select(
            AnonymousExpression.create(PersonResult.class)
            .with(pr.name, p.firstname)
            .with(pr.count, 1));

    assertEquals("docs.Persons.Select(person => new {Name = person.firstname, Count = 1})", select.toLinq());

    Grouping<StringPath> grouping = Grouping.create(StringPath.class);

    IndexExpression reduce = from("results")
      .groupBy(pr.name)
      .select(
          AnonymousExpression.create(PersonResult.class)
          .with(pr.name, grouping.key)
          .with(pr.count, grouping.sum(pr.count).divide(grouping.sum(pr.count))));

    assertEquals("results.GroupBy(personResult => personResult.name)." +
    		"Select(group => new {Name = group.key, Count = group.Sum(personResult => personResult.count) / group.Sum(personResult => personResult.count)})", reduce.toLinq());

  }


  @Test
  public void testOrder() {
    QPerson p = QPerson.person;
    IndexExpression query1 = from(Person.class)
      .orderBy(p.firstname.asc(), p.lastname.desc());

    assertEquals("docs.Persons.OrderBy(person => person.firstname).OrderByDescending(person => person.lastname)", query1.toLinq());

    IndexExpression query2 = from(Person.class)
        .orderBy(p.firstname.asc()).orderBy(p.lastname.desc());

      assertEquals("docs.Persons.OrderBy(person => person.firstname).OrderByDescending(person => person.lastname)", query2.toLinq());

  }

  @Test
  public void testAnonymousExpression() {
    QPersonResult pr = QPersonResult.personResult;
    QPerson p = QPerson.person;
    AnonymousExpression<PersonResult> anonymousExpression =
        AnonymousExpression.create(PersonResult.class)
        .with(pr.name, p.firstname)
        .with(pr.count, 1);

    LinqSerializer serializer = new LinqSerializer(LinqQueryTemplates.DEFAULT);
    anonymousExpression.accept(serializer, null);
    assertEquals("new {Name = person.firstname, Count = 1}", serializer.toString());

  }

  @Test
  public void testGroupBy() {
    QCompany c = QCompany.company;
    QPersonResult pr = QPersonResult.personResult;
    QPerson p = QPerson.person;

    assertEquals("docs.Companies.GroupBy(company => company.name)",
        from(Company.class)
        .groupBy(c.name)
        .toLinq());

    assertEquals("docs.Companies.GroupBy(c => c.name)",
        from(Company.class)
        .groupBy(StringTemplate.create("c => c.name"))
        .toLinq());

    assertEquals("docs.Persons.GroupBy(person => new {Name = person.firstname, Count = 1})",
        from(Person.class)
        .groupBy(AnonymousExpression.create(PersonResult.class)
            .with(pr.name, p.firstname)
            .with(pr.count, Expressions.constant(1)))
        .toLinq());

    Grouping<QPersonResult> grouping = Grouping.create(QPersonResult.class, "group");

    assertEquals("docs.Persons.GroupBy(person => new {Name = person.firstname, Count = 1}).Select(group => new {Name = group.key.name, Count = group.Sum(personResult => personResult.count)})",
        from(Person.class)
        .groupBy(AnonymousExpression.create(PersonResult.class)
            .with(pr.name, p.firstname)
            .with(pr.count, 1))
        .select(AnonymousExpression.create(PersonResult.class)
            .with(pr.name, grouping.key.name)
            .with(pr.count, grouping.sum(pr.count)))
        .toLinq());

  }



  @Test
  public void testGrouping() {
    QPersonResult personResult = new QPersonResult("pr");
    Grouping<StringPath> grouping = Grouping.create(StringPath.class);
    NumberExpression<Integer> numberExpression = grouping.sum(personResult.count);

    LinqSerializer serializer = new LinqSerializer(LinqQueryTemplates.DEFAULT);
    numberExpression.accept(serializer, null);
    assertEquals("group.Sum(pr => pr.count)", serializer.toString());

  }


}
