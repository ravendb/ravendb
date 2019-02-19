using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class AnalyzerResolution : RavenTestBase
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
                store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition }));

            }
        }
    }
}
