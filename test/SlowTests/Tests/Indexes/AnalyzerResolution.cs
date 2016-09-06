using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
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
                store.DatabaseCommands.PutIndex("test", new IndexDefinitionBuilder<User>
                {
                    Map = docs => from doc in docs select new { doc.Id },
                    Analyzers = { { x => x.Id, "SimpleAnalyzer" } }
                });
            }
        }
    }
}
