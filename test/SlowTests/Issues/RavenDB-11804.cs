using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11804 : RavenTestBase
    {
        [Fact]
        public void ShouldThrowUnknownAlias1()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and a.Category = 'test' " +
                                                            "and a.Event = 'processed' " +
                                                            "order by a.Timestamp desc " +
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "Event: a.Event, DateA: new Date(x.Timestamp * 1000)}"); // unknown alias x
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias2()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and x.Category = 'test' " + // unknown alias x
                                                            "and a.Event = 'processed' " +
                                                            "order by a.Timestamp desc " +
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "Event: a.Event, DateA: new Date(a.Timestamp * 1000)}");
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias3()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and a.Category = 'test' " +
                                                            "and a.Event = 'processed' " +
                                                            "order by x.Timestamp desc " + // unknown alias x
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "Event: a.Event, DateA: new Date(a.Timestamp * 1000)}");
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias4()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and a.Category = 'test' " +
                                                            "and a.Event = 'processed' " +
                                                            "order by a.Timestamp desc " +
                                                            "select x.Email as E"); // unknown alias x
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias5()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "group by x.Category " + // unknown alias x
                                                            "select a.Category as Cat, count()");
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias6()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and a.Category = 'test' " +
                                                            "load x.Company as C " + // unknown alias x
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "Event: a.Event, Company: C.Name }");
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias7()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "and a.Category = 'test' " +
                                                            "and a.Event = 'processed' " +
                                                            "load a.Company as x " +
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "CompanyName: x.Name, DateA: new Date(y.Timestamp * 1000)}"); // unknown alias y
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias y, but there are aliases specified in the query (a, x)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias8()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<object>("from EmailEvents as a " +
                                                            "where a.Email = 'email' " +
                                                            "select { Email: a.Email, Category: a.Category[0], " +
                                                            "Event: a.Event, DateA: new Date(a.Timestamp * 1000)}" +
                                                            "include x.Company"); // unknown alias x
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias x, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowUnknownAlias9()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Advanced
                        .RawQuery<object>("from EmailEvents as a " +
                                          "where a.Email = 'email' " +
                                          "select { Email: a.Email, Category: $category.Name, Event: $event.Date }") // unknown alias $event
                        .AddParameter("category", new { Name = "support" });
                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());

                    Assert.Contains("Unknown alias $event, but there are aliases specified in the query (a)", ex.Message);

                }
            }
        }
    }
}
