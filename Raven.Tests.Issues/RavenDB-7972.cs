using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using System.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_7972 : RavenTest
    {
        [Fact]
        public void SearchingWithDoubleDashesShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                var index = new FooByBar();
                index.Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Bar = "I should // be able to query this" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var res = session.Query<Foo>(index.IndexName).Search(x => x.Bar, "I should // be able to query this").Single();
                }
            }
        }

        private class FooByBar : AbstractIndexCreationTask<Foo>
        {
            public FooByBar()
            {
                Map = foos => from foo in foos select new { foo.Bar };
                Indexes.Add(x => x.Bar, Abstractions.Indexing.FieldIndexing.Analyzed);
            }
        }
        private class Foo
        {
            public string Bar { get; set; }
        }
    }
}
