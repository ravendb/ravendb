using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25245(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void ValidationOfEmbeddingsTransformationShouldThrow()
    {
        using (var store = GetDocumentStore())
        {
            const string script = """
                                  embeddings.generate({
                                                // No chunking method is specified here
                                                Name: this.Name,
                                                Description: this.Description
                                            });
                                  """;

            var chunkingOptions = new ChunkingOptions()
            {
                ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
                MaxTokensPerChunk = 2048,
                OverlapTokens = 128 // unsupported for this method
            };

            var exception = Assert.Throws<RavenException>(() => AddEmbeddingsGenerationTask(store, script: script, chunkingOptionsForScript: chunkingOptions));
            
            Assert.Contains("'embeddings.generate': OverlapTokens option is only supported for the following chunking methods: MarkDownSplitParagraphs, PlainTextSplitParagraphs.", exception.Message);
        }
    }
}
