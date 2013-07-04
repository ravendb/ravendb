package raven.linq;

import static org.junit.Assert.assertEquals;
import static raven.linq.dsl.IndexDefinitionBuilder.from;

import org.junit.Test;

import raven.linq.dsl.Grouping;
import raven.linq.dsl.IndexDefinitionBuilder;
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
import com.mysema.query.types.ConstructorExpression;
import com.mysema.query.types.OperatorImpl;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.NumberExpression;
import com.mysema.query.types.expr.SimpleExpression;
import com.mysema.query.types.expr.SimpleOperation;
import com.mysema.query.types.path.SimplePath;
import com.mysema.query.types.path.StringPath;
import com.mysema.query.types.template.NumberTemplate;
import com.mysema.query.types.template.StringTemplate;


public class QuerySample {

  @Test
  public void testMapReduce() {
    QPerson p = QPerson.person;
    QPersonResult pr = QPersonResult.personResult;
    IndexDefinitionBuilder select = from(Person.class)
        .select(
            AnonymousExpression.create(PersonResult.class)
            .with(pr.name, p.firstname)
            .with(pr.count, 1));

    System.out.println(select);

    Grouping<StringPath> grouping = Grouping.create(StringPath.class);

    IndexDefinitionBuilder reduce = from("results")
      .groupBy(pr.name)
      .select(
          AnonymousExpression.create(PersonResult.class)
          .with(pr.name, grouping.key)
          .with(pr.count, grouping.sum(pr.count).divide(grouping.sum(pr.count))));

    System.out.println(reduce);

  }


  @Test
  public void testOrder() {
    QPerson p = QPerson.person;
    IndexDefinitionBuilder query1 = from(Person.class)
      .orderBy(p.firstname.asc(), p.lastname.desc());

    assertEquals("docs.Persons.OrderBy(person => person.firstname).OrderByDescending(person => person.lastname)", query1.toLinq());

    IndexDefinitionBuilder query2 = from(Person.class)
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
  public void testAny() {
    QCompany c = new QCompany("c");
    QCompany d = new QCompany("d");

    QPersonResult pr = QPersonResult.personResult;
    IndexDefinitionBuilder query1 = from(Company.class)
        .where(d.name.eq("Test").and(d.name.eq("AA")))
        .where(d.employees.size().gt(d.id))
        .select(AnonymousExpression.create(PersonResult.class)
            .with(pr.name, d.name));

    System.out.println(query1.toLinq());

    System.out.println(query1);

  }

  @Test
  public void testTranformAny() {

    QCompany c = new QCompany("c");

    IndexDefinitionBuilder query = from(Company.class)
      .where(c.employees.any().pets.any().name.eq("Dog"));


    BooleanExpression booleanExpression = c.employees.any().pets.any().name.eq("Dog");

    OperatorImpl<String> lambda = new OperatorImpl<>("LAMBDA");

    SimplePath<Person> person = Expressions.path(Person.class, "p");
    SimplePath<String> firstName = Expressions.path(String.class, person, "firstName");
    SimpleExpression<?> lamda = SimpleOperation.create(firstName.getType(), lambda, person, firstName);

    System.out.println(lamda);


    System.out.println(booleanExpression);


    System.out.println(query);

//    LinqQuery<Company> query1 = from(Company.class)
//        .where(c.employees.any().age));

//    System.out.println(query1);


  }

  @Test
  public void test() {
    String linq = from(Person.class).toLinq();
    assertEquals("docs.Persons", linq);

    QPerson p = new QPerson("p");
    QCompany c = new QCompany("c");
    QPersonResult pr = QPersonResult.personResult;

    IndexDefinitionBuilder query1 = from(Company.class)
        .where(c.name.startsWith("C"))
        .where(c.name.endsWith("a"))
        .orderBy(c.name.asc())
        .orderBy(c.id.asc())
        .selectMany(c.employees)
        .where(p.firstname.eq("Marcin"))
        .where(p.lastname.length().gt(7));

    IndexDefinitionBuilder select = from(Person.class)
      .where(p.firstname.length().lt(5))
      .select(QPersonResult.create(p.firstname, NumberTemplate.ONE));
    System.out.println("SELECT: " + select);


    Grouping<StringPath> grouping = Grouping.create(StringPath.class, "group");

    System.out.println("PATH: " + grouping.key);



    ConstructorExpression<PersonResult> personResult = QPersonResult.create(p.firstname, p.age);
    System.out.println(personResult);

    String linq2 = query1.toLinq();
    System.out.println(linq2);
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
