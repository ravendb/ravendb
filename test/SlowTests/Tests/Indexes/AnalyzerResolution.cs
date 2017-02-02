using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class AnalyzerResolution : RavenNewTestBase
    {
        [Fact]
        public void can_resolve_internal_analyzer()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinitionBuilder<User>
                {
                    Map = docs => from doc in docs select new { doc.Id },
                    Analyzers = { { x => x.Id, "SimpleAnalyzer" } }
                }.ToIndexDefinition(store.Conventions)));
            }
        }
    }
}
