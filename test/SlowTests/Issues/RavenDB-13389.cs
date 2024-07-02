using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Patch;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_13389 : RavenTestBase
{
    public RavenDB_13389(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task TestDatabaseContextScript()
    {
        using (var store = GetDocumentStore())
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            
            var scriptResult = new AdminJsConsole(Server, database).ApplyScript(new AdminJsScript(
                "databaseCtx.OpenReadTransaction();\n\nreturn database.DocumentsStorage.GetNumberOfDocuments(databaseCtx)"));

            Assert.Equal("{\"Result\":0}", scriptResult);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task TestClusterContextScript()
    {
        using (var store = GetDocumentStore())
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            
            var scriptResult = new AdminJsConsole(Server, database).ApplyScript(new AdminJsScript(
                "clusterCtx.OpenReadTransaction();\n\nreturn server.ServerStore.Engine.ReadNodeTag(clusterCtx);"));

            Assert.Equal("{\"Result\":\"A\"}", scriptResult);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task TestServerContextScript()
    {
        using (var store = GetDocumentStore())
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            
            var scriptResult = new AdminJsConsole(Server, database).ApplyScript(new AdminJsScript(
                $"serverCtx.OpenReadTransaction();\n\nreturn server.ServerStore.Cluster.GetServerWideTaskNameByTaskId(serverCtx, 'foo', 1);"));

            Assert.Equal("{\"Result\":null}", scriptResult);
        }
    }
}
