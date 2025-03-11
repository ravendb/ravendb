using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class ChunkingTests : RavenTestBase
{
    public ChunkingTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanChunkByWhitespace(Options options)
    {
        const string text = "some long text that will produce multiple chunks and also contains numbers like 0.5%, 0.1f, 20, 2000";
        var chunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 4 };
        
        var result = TextChunker.ChunkValue(text, chunkingOptions);

        Assert.Equal(5, result.Count);
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanChunkByWhitespaceWithExactNumberOfTokens(Options options)
    {
        const string text = "some long text abc";
        var chunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 4 };
        
        var result = TextChunker.ChunkValue(text, chunkingOptions);

        Assert.Equal(1, result.Count);
        Assert.Equal(text, result.First());
    }
}
