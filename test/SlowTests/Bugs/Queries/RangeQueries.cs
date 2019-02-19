using System;
using System.Collections.Generic;
using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.Queries
{
    public class RangeQueries : RavenTestBase
    {
        [Fact]
        public void LinqTranslateCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<WithInteger>()
                        .Where(x => x.Sequence < 300 && x.Sequence > 150);

                    var query = RavenTestHelper.GetIndexQuery(q);

                    Assert.Equal("from WithIntegers where Sequence > $p0 and Sequence < $p1", query.Query);
                    Assert.Equal(150, query.QueryParameters["p0"]);
                    Assert.Equal(300, query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void LinqTranslateCorrectly_Reverse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<WithInteger>()
                        .Where(x => 150 > x.Sequence && x.Sequence < 300);
                    
                    var query = RavenTestHelper.GetIndexQuery(q);

                    Assert.Equal("from WithIntegers where Sequence > $p0 and Sequence < $p1", query.Query);
                    Assert.Equal(150, query.QueryParameters["p0"]);
                    Assert.Equal(300, query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void LinqTranslateCorrectly_Reverse2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<WithInteger>()
                        .Where(x => 150 > x.Sequence && 300 < x.Sequence);

                    var query = RavenTestHelper.GetIndexQuery(q);

                    Assert.Equal("from WithIntegers where Sequence > $p0 and Sequence < $p1", query.Query);
                    Assert.Equal(150, query.QueryParameters["p0"]);
                    Assert.Equal(300, query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void LinqTranslateCorrectlyEquals()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<WithInteger>()
                        .Where(x => x.Sequence >= 150 && x.Sequence <= 300);

                    var query = RavenTestHelper.GetIndexQuery(q);
                    
                    Assert.Equal("from WithIntegers where Sequence between $p0 and $p1", query.Query);
                    Assert.Equal(150, query.QueryParameters["p0"]);
                    Assert.Equal(300, query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void CanQueryOnRangeEqualsInt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WithInteger { Sequence = 1 });
                    session.Store(new WithInteger { Sequence = 2 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var withInt = session.Query<WithInteger>().Where(x => x.Sequence >= 1).ToArray();
                    Assert.Equal(2, withInt.Length);
                }
            }
        }

        [Fact]
        public void CanQueryOnRangeEqualsLong()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WithLong { Sequence = 1 });
                    session.Store(new WithLong { Sequence = 2 });

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var withLong = session.Query<WithLong>().Where(x => x.Sequence >= 1).ToArray();
                    Assert.Equal(2, withLong.Length);
                }
            }
        }

        [Fact]
        public void CanQueryOnRangeInt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WithInteger { Sequence = 1 });
                    session.Store(new WithInteger { Sequence = 2 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var withInt = session.Query<WithInteger>().Where(x => x.Sequence > 0).ToArray();
                    Assert.Equal(2, withInt.Length);
                }
            }
        }

        [Fact]
        public void CanQueryOnRangeLong()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WithLong { Sequence = 1 });
                    session.Store(new WithLong { Sequence = 2 });

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var withLong = session.Query<WithLong>().Where(x => x.Sequence > 0).ToArray();
                    Assert.Equal(2, withLong.Length);
                }
            }
        }

        [Fact]
        public void CanQueryOnRangeDoubleAsPartOfIDictionary()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { @"from doc in docs.UserWithIDictionaries
                                from nestedValue in doc.NestedItems
                                select new {Key=nestedValue.Key, Value=nestedValue.Value.Value}" },
                    Name = "SimpleIndex"
                }}));

                using (var s = store.OpenSession())
                {
                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem> 
                                {
                                    { "Color", new NestedItem { Value = 10 } }
                                }
                    });

                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem> 
                                {
                                    { "Color", new NestedItem { Value = 20 } }
                                }
                    });

                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem> 
                                {
                                    { "Color", new NestedItem { Value = 30 } }
                                }
                    });

                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem>
                                {
                                    { "Color", new NestedItem { Value = 150 } }
                                }
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<UserWithIDictionary>("SimpleIndex")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(5))
                        .WhereEquals("Key", "Color")
                        .AndAlso()
                        .WhereGreaterThan("Value", 20.0d)
                        .ToArray();

                    Assert.Equal(2, users.Count());
                }
            }
        }

        private class WithInteger
        {
            public int Sequence { get; set; }
        }
        private class WithLong
        {
            public long Sequence { get; set; }
        }

        private class UserWithIDictionary
        {
            public string Id { get; set; }
            public IDictionary<string, string> Items { get; set; }
            public IDictionary<string, NestedItem> NestedItems { get; set; }
        }

        private class NestedItem
        {
            public string Name { get; set; }
            public double Value { get; set; }
        }
    }
}
