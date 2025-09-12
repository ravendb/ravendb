using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using SlowTests.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_24816 : EmbeddingsGenerationTestBase
{
    public RavenDB_24816(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void OverlapTokensSettingShouldWork()
    {
        const string plainTextToChunk = "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";
        var expectedChunks = new List <string>()
        {
            "this is a relatively long text",
            "relatively long text that should",
            "that should produce multiple",
            "produce multiple chunks because",
            "chunks because of the chunking",
            "of the chunking configuration (max",
            "configuration (max tokens per chunk)",
            "tokens per chunk)"
        };

        var chunkingOptions = new ChunkingOptions()
        {
            ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs,
            MaxTokensPerChunk = 10,
            OverlapTokens = 5
        };
        
        var chunks = Raven.Server.Documents.AI.TextChunker.Chunk(plainTextToChunk, chunkingOptions);
        
        Assert.Equal(expectedChunks, chunks);
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void OverlapTokensSettingForMarkdownShouldWork()
    {
        const string markdownToChunk = """
                                        ## This is an example markdown content
                                            - Text text
                                            - Text text text
                                        
                                        ### More content
                                            this is some text
                                        """;
        var expectedChunks = new List <string>()
        {
            "## This is an example markdown",
            "example markdown content\n    -",
            "content\n    - Text text",
            "Text text - Text text",
            "- Text text text\n\n### More",
            "text\n\n### More content\n    this",
            "content\n    this is some text",
            "is some text"
        };

        var chunkingOptions = new ChunkingOptions()
        {
            ChunkingMethod = ChunkingMethod.MarkDownSplitParagraphs,
            MaxTokensPerChunk = 10,
            OverlapTokens = 5
        };
        
        var chunks = Raven.Server.Documents.AI.TextChunker.Chunk(markdownToChunk, chunkingOptions);
        
        Assert.Equal(expectedChunks, chunks);
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void OverlapTokensSettingInScriptShouldWork()
    {
        const string plainTextToChunk =
            "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";
        string[] expectedChunks = [
            "this is a relatively long text",
            "relatively long text that should",
            "that should produce multiple",
            "produce multiple chunks because",
            "chunks because of the chunking",
            "of the chunking configuration (max",
            "configuration (max tokens per chunk)",
            "tokens per chunk)"
        ];

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                script: "embeddings.generate({ ChunkedName: text.splitParagraphs(this.Name, 10, 5) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = WaitForEmbeddingsGenerationWorkerToRegister(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void OverlapTokensSettingViaPathShouldWork()
    {
        const string plainTextToChunk =
            "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";
        string[] expectedChunks = [
            "this is a relatively long text",
            "relatively long text that should",
            "that should produce multiple",
            "produce multiple chunks because",
            "chunks because of the chunking",
            "of the chunking configuration (max",
            "configuration (max tokens per chunk)",
            "tokens per chunk)"
        ];

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                embeddingsPaths: 
                [
                    new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = new ChunkingOptions()
                    {
                        ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs,
                        MaxTokensPerChunk = 10,
                        OverlapTokens = 5
                    }}
                ]);

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = WaitForEmbeddingsGenerationWorkerToRegister(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void UnsupportedChunkingMethodsThrowRelevantException()
    {
        const string plainTextToChunk =
            "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var exception = Assert.Throws<RavenException>(() => AddEmbeddingsGenerationTask(store,
                embeddingsPaths:
                [
                    new EmbeddingPathConfiguration()
                    {
                        Path = "Name", ChunkingOptions = new ChunkingOptions()
                        {
                            ChunkingMethod = ChunkingMethod.PlainTextSplit,
                            MaxTokensPerChunk = 10,
                            OverlapTokens = 5
                        }
                    }
                ]));

            Assert.Contains("OverlapTokens option is only supported for the following chunking methods: MarkDownSplitParagraphs, PlainTextSplitParagraphs.", exception.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void UnsupportedChunkingSettingsInScriptThrowRelevantException()
    {
        const string plainTextToChunk =
            "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            var (configuration, _) = AddEmbeddingsGenerationTask(store,
                script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 10, 5) });");

            aiTaskDone.Wait(TimeSpan.FromSeconds(5));

            var transformationError = Etl.TryGetTransformationErrorAsync(store.Database, configuration).GetAwaiter().GetResult();

            Assert.Contains("text.splitLines(text | [text], maxTokensPerLine) has to be called with 2 arguments", transformationError.Error);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<string> Names { get; set; }
    }
}
