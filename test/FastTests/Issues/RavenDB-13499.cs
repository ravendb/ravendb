using System;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13499 : RavenTestBase
    {
        public RavenDB_13499(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Alias_in_edge_relationships_select_clause_should_work()
        {
            using (var store = GetDocumentStore())
            {
                var smuggler = new DatabaseSmuggler(store);
                var op = await smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), "./Issues/RavenDB-13499.ravendbdump");
                var _ = await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                
                using (var session = store.OpenSession())
                {
                    var results1 = session.Advanced
                            .GraphQuery<JObject>("match (Person as f)-[Relationships as r select TargetId]->(Person as t)")
                            .ToArray();

                    var results2 = session.Advanced
                            .GraphQuery<JObject>("match (Person as f)-[Relationships as r select r.TargetId]->(Person as t)")
                            .ToArray();

                    Assert.Equal(results1, results2);
                }
            }
        }
    }
}
