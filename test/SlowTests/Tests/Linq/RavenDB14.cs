using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;

using Xunit;

namespace SlowTests.Tests.Linq
{
    public class RavenDB14 : RavenNewTestBase
    {
        private readonly List<string> _queries = new List<string>();

        private class User
        {
            public string Name { get; set; }

            public bool Active { get; set; }
        }

        [Fact(Skip = "RavenDB-6264")]
        public void WhereThenFirstHasAND()
        {
            using (var store = GetDocumentStore())
            {
                //store.RegisterListener(new RecordQueriesListener(_queries));
                var documentSession = store.OpenSession();
                var _ = documentSession.Query<User>().Where(x => x.Name == "ayende").FirstOrDefault(x => x.Active);

                Assert.Equal("Name:ayende AND Active:true", _queries[0]);
            }
        }

        [Fact(Skip = "RavenDB-6264")]
        public void WhereThenSingleHasAND()
        {
            using (var store = GetDocumentStore())
            {
                //store.RegisterListener(new RecordQueriesListener(_queries));
                var documentSession = store.OpenSession();
                var _ = documentSession.Query<User>().Where(x => x.Name == "ayende").SingleOrDefault(x => x.Active);

                Assert.Equal("Name:ayende AND Active:true", _queries[0]);
            }
        }

        //private class RecordQueriesListener : IDocumentQueryListener
        //{
        //    private readonly List<string> _queries;

        //    public RecordQueriesListener(List<string> queries)
        //    {
        //        _queries = queries;
        //    }

        //    public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
        //    {
        //        _queries.Add(queryCustomization.ToString());
        //    }
        //}
    }
}
