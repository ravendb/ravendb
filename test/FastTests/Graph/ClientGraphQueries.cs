using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Linq;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Graph
{
    public class ClientGraphQueries : RavenTestBase
    {
        public ClientGraphQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGraphQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var bar = new Bar {Name = "Barvazon"};
                    var barId = "Bars/1";
                    session.Store(bar, barId);
                    session.Store(new Foo {Name = "Foozy", Bars = new List<string> {barId}});
                    session.SaveChanges();
                    FooBar res = session.Advanced.GraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)").With("Foo", session.Query<Foo>()).Single();
                    Assert.Equal(res.Foo.Name, "Foozy");
                    Assert.Equal(res.Bar.Name, "Barvazon");
                }
            }
        }


        [Fact]
        public void CanAggregateQueryParametersProperly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var bar = new Bar {Name = "Barvazon", Age = 19};
                    var barId = "Bars/1";
                    session.Store(bar, barId);

                    session.Store(new Foo {Name = "Foozy", Bars = new List<string> {barId}});
                    session.SaveChanges();

                    foreach (var names in new[]
                    {
                        new[] {"Fi", "Fah", "Foozy"}, new[] {"Fi", "Foozy", "Fah"}, new[] {"Foozy", "Fi", "Fah", "Foozy"},
                        new[] {"Fi", "Foozy", "Fah", "Fah", "Foozy"},
                    })
                    {
                        var res = session.Advanced.GraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                            .With("Foo", builder => builder.DocumentQuery<Foo>().WhereIn(x => x.Name, names))
                            .With("Bar", session.Query<Bar>().Where(x => x.Age >= 18))
                            .WaitForNonStaleResults()
                            .ToList();

                        Assert.Single(res);
                        Assert.Equal(res[0].Foo.Name, "Foozy");
                        Assert.Equal(res[0].Bar.Name, "Barvazon");
                    }
                }
            }
        }

        [Fact]
        public void WaitForNonStaleResultsOnGraphQueriesWithClauseShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var names = new[] {"Fi", "Fah", "Foozy"};

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.GraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                        .With("Foo", builder => builder.DocumentQuery<Foo>().WhereIn(x => x.Name, names).WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
                        .With("Bar", session.Query<Bar>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Age >= 18))
                        .WaitForNonStaleResults()
                        .GetIndexQuery();

                    Assert.True(query.WaitForNonStaleResults);
                    Assert.Equal(TimeSpan.FromMinutes(5), query.WaitForNonStaleResultsTimeout);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncGraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                        .With("Foo", builder => builder.AsyncDocumentQuery<Foo>().WhereIn(x => x.Name, names).WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
                        .With("Bar", session.Query<Bar>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Age >= 18))
                        .WaitForNonStaleResults()
                        .GetIndexQuery();

                    Assert.True(query.WaitForNonStaleResults);
                    Assert.Equal(TimeSpan.FromMinutes(5), query.WaitForNonStaleResultsTimeout);
                }
            }
        }

        [Fact]
        public void CanUseWithEdges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var now = DateTime.UtcNow;

                    session.Store(
                        new Friend
                        {
                            Name = "F1",
                            Age = 21,
                            Friends = new[]
                            {
                                new FriendDescriptor {FriendId = "Friend/2", FriendsSince = now - TimeSpan.FromDays(1024)},
                                new FriendDescriptor {FriendId = "Friend/3", FriendsSince = now - TimeSpan.FromDays(678)},
                                new FriendDescriptor {FriendId = "Friend/4", FriendsSince = now - TimeSpan.FromDays(345)}
                            }
                        }, "Friend/1");
                    session.Store(
                        new Friend
                        {
                            Name = "F2",
                            Age = 19,
                            Friends = new[]
                            {
                                new FriendDescriptor {FriendId = "Friend/1", FriendsSince = now - TimeSpan.FromDays(1024)},
                                new FriendDescriptor {FriendId = "Friend/4", FriendsSince = now - TimeSpan.FromDays(304)}
                            }
                        }, "Friend/2");
                    session.Store(
                        new Friend {Name = "F3", Age = 41, Friends = new[] {new FriendDescriptor {FriendId = "Friend/1", FriendsSince = now - TimeSpan.FromDays(678)}}},
                        "Friend/3");
                    session.Store(
                        new Friend
                        {
                            Name = "F4",
                            Age = 32,
                            Friends = new[]
                            {
                                new FriendDescriptor {FriendId = "Friend/2", FriendsSince = now - TimeSpan.FromDays(304)},
                                new FriendDescriptor {FriendId = "Friend/1", FriendsSince = now - TimeSpan.FromDays(345)}
                            }
                        }, "Friend/4");

                    var from = now.AddDays(-345);

                    session.SaveChanges();
                    var res = session.Advanced.GraphQuery<FriendsTuple>("match (F1)-[L1]->(F2)")
                        .With("F1", session.Query<Friend>())
                        .With("F2", session.Query<Friend>())
                        .WithEdges("L1", "Friends", $"where FriendsSince >= '{from.GetDefaultRavenFormat(isUtc: true)}' select FriendId")
                        .WaitForNonStaleResults()
                        .OrderByDescending(x => x.F1.Age)
                        .Select(x => x.F1)
                        .ToList();
                    Assert.Equal(res.Count, 4);
                    Assert.Equal(res[0].Name, "F4");
                    Assert.Equal(res[1].Name, "F4");
                    Assert.Equal(res[2].Name, "F1");
                    Assert.Equal(res[3].Name, "F2");
                }
            }
        }

        private class FriendsTuple
        {
            public Friend F1 { get; set; }
            public FriendDescriptor L1 { get; set; }
            public Friend F2 { get; set; }
        }

        private class Friend
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public FriendDescriptor[] Friends { get; set; }
        }

        private class FooBar
        {
            public Foo Foo { get; set; }
            public Bar Bar { get; set; }
        }

        private class Foo
        {
            public string Name { get; set; }
            public List<string> Bars { get; set; }
        }

        private class Bar
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        private class FriendDescriptor
        {
            public DateTime FriendsSince { get; set; }
            public string FriendId { get; set; }
        }
    }
}
