using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24613_MultipleEmbeddingsGenerateCalls(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task MultipleEmbeddingsGenerateCallsShouldCreateAllEmbeddings()
    {
        using var store = GetDocumentStore();

        // Create a test document with data for multiple embeddings
        var testDoc = new TestDocument
        {
            Summary = "This is a test document summary", KeyWords = "test, document, summary, keywords", Content = "This is the main content of the test document"
        };

        string docId;
        using (var session = store.OpenSession())
        {
            session.Store(testDoc);
            session.SaveChanges();
            docId = testDoc.Id;
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);

        // Create embeddings generation task with script that has multiple embeddings.generate() calls
        var script = @"
                // BUG: Multiple embeddings.generate() calls - only the last one actually works
                // Expected: All three embeddings should be created
                // Actual: Only the last embedding (third_embedding) is created
                embeddings.generate({
                    'first_embedding': this.Summary || 'default summary'
                });
                
                embeddings.generate({
                    'second_embedding': this.KeyWords || 'default keywords'
                });
                
                embeddings.generate({
                    'third_embedding': this.Content || 'default content'
                });
            ";

        var (config, connection) = AddEmbeddingsGenerationTask(store, script: script, collectionName: "TestDocuments");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // All three embeddings should be created, but due to the bug, only the last one will exist
        // This test will fail until the bug is fixed

        // This assertion should pass - the last call works
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "third_embedding", [testDoc.Content], docId);

        // These assertions will fail due to the bug - the first two calls are ignored
        // After the bug is fixed, these should pass
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "first_embedding", [testDoc.Summary], docId);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "second_embedding", [testDoc.KeyWords], docId);
    }

    private class TestDocument
    {
        public string Id { get; set; }
        public string Summary { get; set; }
        public string KeyWords { get; set; }
        public string Content { get; set; }
    }
}
