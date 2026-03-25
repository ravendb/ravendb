using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;
#if DEBUG
public class RavenDB_24311(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai | RavenTestCategory.Core, RavenArchitecture.AllX64)]
    public async Task CanAssertEmbeddingsGenerationChangeInSchema()
    {
        using var store = GetDocumentStore(new Options() { RunInMemory = false });

        AddEmbeddingsGenerationTask(store, collectionName: "Dtos");
        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, true));
        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, false));
    }
}
#endif
