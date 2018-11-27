using FastTests;
using System.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12357 : RavenTestBase
    {
        private class MyDoc
        {
            public string Id { get; set; }
        }

        private class WithHasValue
        {
            public bool? HasValue;
            public object SomeProp;
            public string DocId;
        }

        [Fact]
        public void TestProjectingNullProperty1()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MyDoc());
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                        orderby d.Id
                        select new WithHasValue
                        {
                            SomeProp = null,
                            HasValue = false //this creates js projection
                        };

                    Assert.Equal("from MyDocs as d order by id() select { SomeProp : null, HasValue : false }"
                        , query.ToString());

                    var results = query.ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Null(results[0].SomeProp);
                    Assert.False(results[0].HasValue);
                }
            }
        }

        [Fact]
        public void TestProjectingNullProperty2()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MyDoc(), "MyDocs/1");
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    // non-js projection

                    var query = from d in s.Query<MyDoc>()
                        orderby d.Id
                        select new WithHasValue
                        {
                            SomeProp = null,
                            DocId = d.Id
                        };

                    Assert.Equal("from MyDocs order by id() select null as SomeProp, id() as DocId"
                        , query.ToString());

                    var results = query.ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Null(results[0].SomeProp);
                    Assert.Null(results[0].HasValue);
                    Assert.Equal("MyDocs/1", results[0].DocId);
                }
            }
        }
    }
}
