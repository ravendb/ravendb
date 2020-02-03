using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class Contains : RavenTestBase
    {
        public Contains(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDoc
        {
            public string SomeProperty { get; set; }
            public string[] StringArray { get; set; }
        }

        [Fact]
        public void CanQueryArrayWithContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var otherDoc = new TestDoc { SomeProperty = "foo" };
                    var doc = session.Query<TestDoc>()
                        .FirstOrDefault(ar => ar.StringArray.Contains(otherDoc.SomeProperty));
                    Assert.NotNull(doc);
                }
            }
        }

        [Fact]
        public void CanQueryListWithContainsAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();

                    session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    var items = new[] { "a", "b", "c" };
                    var test = session.Query<TestDoc>()
                        .Where(ar => ar.StringArray.ContainsAny(items) && ar.SomeProperty == "somethingElse");

                    var iq = RavenTestHelper.GetIndexQuery(test);
                    Assert.Equal("from 'TestDocs' where StringArray in ($p0) and SomeProperty = $p1", iq.Query);
                    Assert.Equal(items, iq.QueryParameters["p0"]);
                    Assert.Equal("somethingElse", iq.QueryParameters["p1"]);

                    var results = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAny(new[] { "test", "NOTmatch" }))
                                         .ToList();
                    Assert.Equal(1, results.Count);

                    var noResults = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAny(new[] { "NOTmatch", "random" }))
                                         .ToList();
                    Assert.Equal(0, noResults.Count);
                }
            }
        }

        [Fact]
        public void CanQueryListWithContainsAll()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();

                    session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    var items = new[] { "a", "b", "c" };
                    var test = session.Query<TestDoc>()
                        .Where(ar => ar.StringArray.ContainsAll(items) && ar.SomeProperty == "somethingElse");

                    var iq = RavenTestHelper.GetIndexQuery(test);
                    Assert.Equal("from 'TestDocs' where StringArray all in ($p0) and SomeProperty = $p1", iq.Query);
                    Assert.Equal(items, iq.QueryParameters["p0"]);
                    Assert.Equal("somethingElse", iq.QueryParameters["p1"]);

                    var results = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAll(new[] { "test", "doc", "foo" }))
                                         .ToList();
                    Assert.Equal(1, results.Count);

                    var noResults = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAll(new[] { "test", "doc", "foo", "NOTmatch" }))
                                         .ToList();
                    Assert.Equal(0, noResults.Count);
                }
            }
        }

        [Fact]
        public void DoesNotSupportStrings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { SomeProperty = "Ensure that Contains on IEnumerable<Char> is not supported." };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var otherDoc = new TestDoc { SomeProperty = "Contains" };
                    var exception = Assert.Throws<NotSupportedException>(() =>
                    {
                        session.Query<TestDoc>().FirstOrDefault(ar => ar.SomeProperty.Contains(otherDoc.SomeProperty));
                    });
                    Assert.Contains("Contains is not supported, doing a substring match", exception.InnerException.Message);
                }
            }
        }
    }
}
