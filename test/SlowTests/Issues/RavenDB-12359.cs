using FastTests;
using Raven.Client.Documents;
using System.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12359 : RavenTestBase
    {
        private class MyDoc
        {
            public string Id { get; set; }
            public int? NullableInt { get; set; }
        }

        private class WithHasValue
        {
            public bool? HasValue;
            public object SomeProp;
        }

        public void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                s.Store(new MyDoc
                {
                    Id = "1",
                    NullableInt = null
                });
                s.Store(new MyDoc
                {
                    Id = "2",
                    NullableInt = 1
                });
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var docs = s.Query<MyDoc>().OrderBy(i => i.Id).ToList();
                Assert.Equal(docs.Count, 2);
                Assert.Null(docs[0].NullableInt);
                Assert.Equal(1, docs[1].NullableInt);
            }

        }

        [Fact]
        public void CanProjectHasValuePropertyOfNullable()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                        orderby d.Id
                        select new
                        {
                            HasValue = d.NullableInt.HasValue
                        };

                    Assert.Equal("from MyDocs as d order by id() select { HasValue : d.NullableInt != null }"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue);
                    Assert.True(results[1].HasValue);
                }
            }
        }

        [Fact]
        public void CanProjectHasValuePropertyOfNullable2()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                        orderby d.Id
                        select new WithHasValue
                        {
                            HasValue = d.NullableInt.HasValue
                        };

                    Assert.Equal("from MyDocs as d order by id() select { HasValue : d.NullableInt != null }"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue);
                    Assert.True(results[1].HasValue);
                }
            }
        }

        [Fact (Skip = "RavenDB-12359")]
        public void TestGreaterThanOrEqualToZero()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                                select new
                                {
                                    HasValue = d.NullableInt >= 0
                                };

                    // this uses JS projection
                    // d.NullableInt is null
                    // 'null >= 0' evaluates to 'true' in JavaScript

                    var results = query.ToList();

                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue); // fails here
                    Assert.True(results[1].HasValue);
                }
            }
        }
    }
}
