using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Indexes;
using Raven.Client.Operations.Databases.Indexes;
using Raven.Tests.Core.Utils.Entities;
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
                var indexDefinition = new IndexDefinitionBuilder<User>
                {
                    Map = docs => from doc in docs select new { doc.Id },
                    Analyzers = { { x => x.Id, "SimpleAnalyzer" } }
                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "test";
                store.Admin.Send(new PutIndexesOperation(new[] { indexDefinition }));

            }
        }
    }
}
