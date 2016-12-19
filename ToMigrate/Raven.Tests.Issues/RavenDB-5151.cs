using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Core.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5151: RavenTest
    {
        [Fact]
        void CanDoPrefixQueryOnAnalyzedFields()
        {
            using (var store = NewDocumentStore())
            using (var session = store.OpenSession())
            {
                new FooBarIndex().Execute(store);
                session.Store(new Foo { Bar = "Shalom"});
                session.Store(new Foo { Bar = "Salam" });
                session.SaveChanges();
                WaitForIndexing(store);
                var res = session.Query<Foo, FooBarIndex>().Single(x => x.Bar.StartsWith("Sh"));
                Assert.Equal(res.Bar, "Shalom");
            }
        }

        public class Foo
        {
            public string Bar { get; set; }
        }

        public class FooBarIndex : AbstractIndexCreationTask<Foo>
        {
            public FooBarIndex()
            {
                Map = foos => from foo in foos select new {foo.Bar};
                Analyzers.Add(c => c.Bar, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
            }
        }
    }
}
