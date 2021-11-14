using FastTests;
using System.Linq;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12358 : RavenTestBase
    {
        public RavenDB_12358(ITestOutputHelper output) : base(output)
        {
        }

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

        [Theory]
        [JavaScriptEngineClassData]
        public void ProjectionReturnsAllNull(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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
                                    SomeProp = d.NullableInt //null
                                };
                    var results = query.ToList();

                    Assert.Equal(1, results.Count);
                    Assert.NotNull(results[0]);
                    Assert.Null(results[0].SomeProp);
                    Assert.Null(results[0].HasValue);

                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void TestHasValue(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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
                            HasValue = d.NullableInt.HasValue //false
                        };

                    Assert.Equal("from 'MyDocs' as d order by id() select { HasValue : d?.NullableInt != null }"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(1, results.Count);
                    Assert.NotNull(results[0]);
                    Assert.Null(results[0].SomeProp);
                    Assert.False(results[0].HasValue);
                }
            }
        }
    }
}
