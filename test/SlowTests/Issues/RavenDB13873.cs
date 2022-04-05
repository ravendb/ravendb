using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB13873 : RavenTestBase
    {
        public RavenDB13873(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldWork(Options options)
        {
            const int ceiling = 1025;
            using (var store = GetDocumentStore(options))
            {

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < ceiling; i++)
                        session.Store(new Person { Filter = i.ToString() });
                    session.SaveChanges();
                }

                var list = Enumerable.Range(0, ceiling).Select(x => x.ToString()).ToList();

                using (var s = store.OpenSession())
                {
                    var q = from doc in s.Query<Person>()
                        where doc.Filter.In(list)
                        select doc;

                    var results = q.ToList();
                    Assert.Equal(ceiling, results.Count);
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Filter { get; set; }
        }
    }
}
