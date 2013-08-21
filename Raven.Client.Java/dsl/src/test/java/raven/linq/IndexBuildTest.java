package raven.linq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static raven.linq.dsl.IndexExpression.from;

import org.junit.Test;

import raven.linq.dsl.Grouping;
import raven.linq.dsl.IndexExpression;
import raven.linq.dsl.LinqQueryTemplates;
import raven.linq.dsl.LinqSerializer;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.samples.Company;
import raven.samples.Person;
import raven.samples.QCompany;
import raven.samples.QPerson;
import raven.samples.QPersonResult;
import raven.samples.QPet;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.expr.NumberExpression;
import com.mysema.query.types.path.StringPath;
import com.mysema.query.types.template.StringTemplate;


public class IndexBuildTest {

  @Test
  public void testMapReduce() {
    QPerson p = QPerson.person;
    QPersonResult pr = QPersonResult.personResult;
    IndexExpression select = from(Person.class)
        .select(
            new AnonymousExpression()
            .with(pr.name, p.firstname)
            .with(pr.count, 1));

    assertEquals("docs.People.Select(person => new {Name = person.Firstname, Count = 1})", select.toLinq());

    Grouping<StringPath> grouping = Grouping.create(StringPath.class);

    IndexExpression reduce = from("results")
        .groupBy(pr.name)
        .select(
            new AnonymousExpression()
            .with(pr.name, grouping.key)
            .with(pr.count, grouping.sum(pr.count).divide(grouping.sum(pr.count))));

    assertEquals("results.GroupBy(personResult => personResult.Name)." +
        "Select(group => new {Name = group.Key, Count = group.Sum(personResult => personResult.Count) / group.Sum(personResult => personResult.Count)})", reduce.toLinq());

  }


  @Test
  public void testOrder() {
    QPerson p = QPerson.person;
    IndexExpression query1 = from(Person.class)
        .orderBy(p.firstname.asc(), p.lastname.desc());

    assertEquals("docs.People.OrderBy(person => person.Firstname).OrderByDescending(person => person.Lastname)", query1.toLinq());

    IndexExpression query2 = from(Person.class)
        .orderBy(p.firstname.asc()).orderBy(p.lastname.desc());

    assertEquals("docs.People.OrderBy(person => person.Firstname).OrderByDescending(person => person.Lastname)", query2.toLinq());

  }
  @Test
  public void testRootDetectorInSelectMany() {
    QCompany c = QCompany.company;
    QPerson p = QPerson.person;
    try {
      from(Company.class)
      .selectMany(c.employees, p.age);
      fail();
    } catch (Exception e) {
      // ok
    }
  }

  @Test
  public void testTranslator() {
    QCompany c = QCompany.company;
    QPerson p = QPerson.person;
    IndexExpression query1 = from(Company.class)
        .selectMany(c.employees, p)
        .select(p);

    assertEquals("docs.Companies.SelectMany(company => company.Employees, (company, person) => new {Company = company, Person = person}).Select(transId_1 => transId_1.Person)", query1.toLinq());
  }


  @Test
  public void testSelectMany() {
    QCompany c = QCompany.company;
    QPerson p = QPerson.person;
    QPet pet = QPet.pet;

    IndexExpression query1 = from(Company.class)
        .where(c.name.startsWith("C"))
        .where(c.employees.size().lt(10))
        .selectMany(c.employees, p)
        .where(p.firstname.startsWith("A"))
        .where(c.name.length().gt(10))
        .selectMany(p.pets, pet)
        .where(pet.name.startsWith("G"))
        .select(
            new AnonymousExpression().with(pet.name, pet.name)
            );


    assertEquals("docs.Companies." +
        "Where(company => company.Name.StartsWith(\"C\"))" +
        ".Where(company => company.Employees.Length < 10)" +
        ".SelectMany(company => company.Employees, (company, person) => new {Company = company, Person = person})" +
        ".Where(transId_1 => transId_1.Person.Firstname.StartsWith(\"A\"))" +
        ".Where(transId_1 => length(transId_1.Company.Name) > 10)" +
        ".SelectMany(transId_1 => transId_1.Person.Pets, (transId_1, pet) => new {TransId_1 = transId_1, Pet = pet})" +
        ".Where(transId_2 => transId_2.Pet.Name.StartsWith(\"G\")).Select(transId_2 => new {Name = transId_2.Pet.Name})", query1.toLinq());
  }


  @Test
  public void testAnonymousExpression() {
    QPersonResult pr = QPersonResult.personResult;
    QPerson p = QPerson.person;
    AnonymousExpression anonymousExpression =
        new AnonymousExpression()
        .with(pr.name, p.firstname)
        .with(pr.count, 1);

    LinqSerializer serializer = new LinqSerializer(LinqQueryTemplates.DEFAULT);
    anonymousExpression.accept(serializer, null);
    assertEquals("new {Name = person.Firstname, Count = 1}", serializer.toString());

  }

  @Test
  public void testGroupBy() {
    QCompany c = QCompany.company;
    QPersonResult pr = QPersonResult.personResult;
    QPerson p = QPerson.person;

    assertEquals("docs.Companies.GroupBy(company => company.Name)",
        from(Company.class)
        .groupBy(c.name)
        .toLinq());

    assertEquals("docs.Companies.GroupBy(c => c.Name)",
        from(Company.class)
        .groupBy(StringTemplate.create("c => c.Name"))
        .toLinq());

    assertEquals("docs.People.GroupBy(person => new {Name = person.Firstname, Count = 1})",
        from(Person.class)
        .groupBy(new AnonymousExpression()
            .with(pr.name, p.firstname)
            .with(pr.count, Expressions.constant(1)))
            .toLinq());

    Grouping<QPersonResult> grouping = Grouping.create(QPersonResult.class, "group");

    assertEquals("docs.People.GroupBy(person => new {Name = person.Firstname, Count = 1}).Select(group => new {Name = group.Key.Name, Count = group.Sum(personResult => personResult.Count)})",
        from(Person.class)
        .groupBy(new AnonymousExpression()
            .with(pr.name, p.firstname)
            .with(pr.count, 1))
            .select(new AnonymousExpression()
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
    assertEquals("group.Sum(pr => pr.Count)", serializer.toString());

  }


}
