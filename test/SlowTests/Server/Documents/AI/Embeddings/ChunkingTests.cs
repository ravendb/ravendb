using System.Collections.Generic;
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

    [RavenFact(RavenTestCategory.Ai)]
    public void CanChunkByWhitespace()
    {
        const string text = "some long text that will produce multiple chunks and also contains numbers like 0.5%, 0.1f, 20, 2000";
        var expectedChunks = new List<string>() { "some long text that", " will produce multiple chunks", " and also contains numbers", " like 0.5%, 0.1f, 20,", " 2000" };
        
        var chunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 4 };
        
        var result = TextChunker.ChunkValue(text, chunkingOptions);

        Assert.Equal(5, result.Count);
        Assert.Equal(expectedChunks[0], result[0]);
        Assert.Equal(expectedChunks[1], result[1]);
        Assert.Equal(expectedChunks[2], result[2]);
        Assert.Equal(expectedChunks[3], result[3]);
        Assert.Equal(expectedChunks[4], result[4]);
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void CanChunkByWhitespaceWithExactNumberOfTokens()
    {
        const string text = "some long text abc";
        var chunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplit, MaxTokensPerChunk = 4 };
        
        var result = TextChunker.ChunkValue(text, chunkingOptions);

        Assert.Equal(1, result.Count);
        Assert.Equal(text, result.First());
    }
}
