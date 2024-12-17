using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class BlittableVectorIntegrationTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.JavaScript | RavenTestCategory.Patching)]
    public void CanPatchWithBlittableVector()
    {
        using var store = GetDocumentStore();
        var vectorDto = new User(new[] { 0.1f, 0.1f, 0.1f, 0.1f });
        using (var session = store.OpenSession())
        {
            session.Store(vectorDto);
            session.SaveChanges();
        }
        
        var operation = store.Operations.Send(new PatchByQueryOperation("from Users update { this.NewVec = this.Vector; }"));
        operation.WaitForCompletion();

        WaitForUserToContinueTheTest(store);
        using (var session = store.OpenSession())
        {
            var valueFromServer = session.Load<User>(vectorDto.Id);
            Assert.Equal(vectorDto.Vector.Embedding, valueFromServer.NewVec.Embedding);
        }
    }

    private record User(RavenVector<float> Vector, RavenVector<float> NewVec = null, string Id = null);
}
