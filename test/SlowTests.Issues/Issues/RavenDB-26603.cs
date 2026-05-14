using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26603(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void ChunkPlainTextWithContextPrefixShouldPrependPrefixToEachChunk()
    {
        const string plainTextToChunk =
            "RavenDB is a transactional NoSQL document database designed for high performance and operational simplicity. " +
            "It stores JSON documents, indexes them automatically, and supports a rich query language. " +
            "Distributed deployments use Raft consensus to coordinate cluster membership and replication across nodes. " +
            "The storage engine, Voron, organizes pages into B+ trees and applies copy-on-write semantics for durability. " +
            "Full-text search is powered by either Lucene or the newer Corax engine, both pluggable per index.";
        const string prefix = "Doc title: ";

        var withoutPrefix = Raven.Server.Documents.AI.TextChunker.Chunk(plainTextToChunk, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 20
        });

        var withPrefix = Raven.Server.Documents.AI.TextChunker.Chunk(plainTextToChunk, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 20,
            ContextPrefix = prefix
        });

        Assert.NotEmpty(withoutPrefix);
        Assert.NotEmpty(withPrefix);
        Assert.All(withPrefix, c => Assert.StartsWith(prefix, c));
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void ContextPrefixLargerThanMaxTokensShouldThrow()
    {
        const string prefix = "this prefix has a number of tokens that should not fit within the budget";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            Raven.Server.Documents.AI.TextChunker.Chunk("some body text",
                new ChunkingOptions
                {
                    ChunkingMethod = ChunkingMethod.PlainTextSplit,
                    MaxTokensPerChunk = 3,
                    ContextPrefix = prefix
                }));

        Assert.Contains("ContextPrefix is too long", ex.Message);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void OverlapLargerThanEffectiveMaxTokensShouldThrow()
    {
        // MaxTokensPerChunk=10, OverlapTokens=8 is valid by itself (8 <= 10).
        // The prefix consumes enough of the budget that the effective max falls below OverlapTokens.
        const string prefix = "context prefix is here";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            Raven.Server.Documents.AI.TextChunker.Chunk("first paragraph.\n\nsecond paragraph.",
                new ChunkingOptions
                {
                    ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs,
                    MaxTokensPerChunk = 10,
                    OverlapTokens = 8,
                    ContextPrefix = prefix
                }));

        Assert.Contains("OverlapTokens", ex.Message);
        Assert.Contains("effective", ex.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void WhitespaceOnlyContextPrefixShouldFailValidation()
    {
        using var store = GetDocumentStore();

        var chunkingOptions = new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 2048,
            ContextPrefix = "   "
        };

        var exception = Assert.Throws<RavenException>(
            () => AddEmbeddingsGenerationTask(store, script: "embeddings.generate({ Name: this.Name });", chunkingOptionsForScript: chunkingOptions));

        Assert.Contains("ContextPrefix cannot be empty or whitespace-only", exception.Message);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task TextSplitWithContextPrefixInScript()
    {
        const string plainTextToChunk =
            "Vector search retrieves documents based on semantic similarity rather than exact keyword matches. " +
            "Each document is represented by an embedding vector produced by a language model. " +
            "Queries are converted into vectors using the same model and compared via cosine similarity or dot product. " +
            "Approximate nearest neighbor algorithms keep latency low even when the index spans millions of vectors. " +
            "Hybrid retrieval combines vector search with classical BM25 ranking to balance recall and precision.";
        const string prefix = "Doc: ";

        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(plainTextToChunk, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 20,
            ContextPrefix = prefix
        }).ToArray();

        Assert.NotEmpty(expectedChunks);
        Assert.All(expectedChunks, c => Assert.StartsWith(prefix, c));

        var dto = new Dto { Name = plainTextToChunk };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
            script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 20).withContextPrefix('Doc:') });");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        AssertEmbeddingsForPath(store,
            new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
            new AiConnectionStringIdentifier(connectionString.Identifier),
            "ChunkedName", expectedChunks, dto.Id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task TextWithContextInScript()
    {
        const string title = "Document title";
        const string prefix = "Title: ";
        var dto = new Dto { Name = title };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
            script: "embeddings.generate({ Field: this.Name.withContextPrefix('Title: ') });");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        AssertEmbeddingsForPath(store,
            new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
            new AiConnectionStringIdentifier(connectionString.Identifier),
            "Field", [prefix + title], dto.Id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task TextSplitParagraphsWithOverlapAndContextPrefixInScript()
    {
        const string body =
            "Embeddings translate text into dense numeric vectors that capture semantic meaning. " +
            "Two passages with related concepts will land close together in vector space even when they share no exact words.\n\n" +
            "Chunking breaks long documents into smaller passages so each embedding focuses on a single coherent idea. " +
            "Without chunking, important details deep inside a page can be diluted by unrelated surrounding content.\n\n" +
            "A context prefix attached to every chunk preserves document-level signals such as the title or author, " +
            "letting the retrieval layer disambiguate snippets that would otherwise look identical across sources.";
        const string prefix = "P: ";

        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(body, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs,
            MaxTokensPerChunk = 20,
            OverlapTokens = 4,
            ContextPrefix = prefix
        }).ToArray();

        Assert.NotEmpty(expectedChunks);
        Assert.All(expectedChunks, c => Assert.StartsWith(prefix, c));

        var dto = new Dto { Name = body };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
            script: "embeddings.generate({ Paragraphs: text.splitParagraphs(this.Name, 20, 4).withContextPrefix('P: ') });");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        AssertEmbeddingsForPath(store,
            new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
            new AiConnectionStringIdentifier(connectionString.Identifier),
            "Paragraphs", expectedChunks, dto.Id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task ObjectWideContextPrefixOnEmbeddingsGenerate()
    {
        const string body =
            "Vector search retrieves documents based on semantic similarity rather than exact keyword matches. " +
            "Each document is represented by an embedding vector produced by a language model. " +
            "Queries are converted into vectors using the same model and compared via cosine similarity or dot product.";
        const string prefix = "Title:";

        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(body, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 20,
            ContextPrefix = prefix
        }).ToArray();

        Assert.NotEmpty(expectedChunks);
        Assert.All(expectedChunks, c => Assert.StartsWith(prefix, c));

        var dto = new Dto { Name = body };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
            script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 20) }).withContextPrefix('Title:');");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        AssertEmbeddingsForPath(store,
            new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
            new AiConnectionStringIdentifier(connectionString.Identifier),
            "ChunkedName", expectedChunks, dto.Id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task ObjectWideContextPrefixDoesNotOverridePerPropertyPrefix()
    {
        const string body =
            "Embeddings translate text into dense numeric vectors that capture semantic meaning. " +
            "Two passages with related concepts will land close together in vector space even when they share no exact words.";
        const string perPropertyPrefix = "Body:";
        const string objectWidePrefix = "Title:";

        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(body, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
            MaxTokensPerChunk = 20,
            ContextPrefix = perPropertyPrefix
        }).ToArray();

        Assert.NotEmpty(expectedChunks);
        Assert.All(expectedChunks, c => Assert.StartsWith(perPropertyPrefix, c));
        Assert.All(expectedChunks, c => Assert.False(c.StartsWith(objectWidePrefix)));

        var dto = new Dto { Name = body };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
            script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 20).withContextPrefix('Body: ') }).withContextPrefix('Title: ');");

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        AssertEmbeddingsForPath(store,
            new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
            new AiConnectionStringIdentifier(connectionString.Identifier),
            "ChunkedName", expectedChunks, dto.Id);
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
