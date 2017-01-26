using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6084: RavenTest
    {
        class Foo
        {
            public string Bar { get; set; }
        }

        class FooByBar : AbstractIndexCreationTask<Foo>
        {
            public FooByBar()
            {
                Map = foos => from foo in foos select new {foo.Bar};
                Index(x=>x.Bar,FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public void CanQueryStopwordsWithPrefix()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new FooByBar());
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo {Bar="Andrew"});
                    session.Store(new Foo { Bar = "boo" });
                    session.SaveChanges();
                    Assert.DoesNotThrow(()=>session.Query<Foo>("FooByBar").Search(x=>x.Bar,"And*", escapeQueryOptions: EscapeQueryOptions.AllowPostfixWildcard).Customize(x=>x.WaitForNonStaleResults()).Single());
                }
            }
        }
    }
}
