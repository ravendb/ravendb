using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Text;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Config;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class GenerateEmbeddingsTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void CanSingleDocumentHaveTwoEmbeddings()
    {
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Name1", SubDto = new SubDto { Name = "Name1" } };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }, new EmbeddingPathConfiguration() { Path = "SubDto.Name", ChunkingOptions = DefaultChunkingOptions }]);
        Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "SubDto.Name", ["Name1"], id);

        aiTaskDone.Reset();
        using (var session = store.OpenSession())
        {
            var dto = session.Load<Dto>(id);
            dto.Name = "Updated";
            session.Store(dto);
            session.SaveChanges();
        }

        Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

        WaitForUserToContinueTheTest(store);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "SubDto.Name", ["Name1"], id);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void DocumentsWithSingleValue()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "Name1" };
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Name1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void DocumentsWithListOfValues()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Names = new List<string> { "Name1", "Name2", "Name3" } };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Names", ["Name1", "Name2", "Name3"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void DocumentsWithNestedPropertyPath()
    {
        using (var store = GetDocumentStore())
        {
            var subDto = new SubDto() { Name = "Subname1" };
            var dto = new Dto { SubDto = subDto };

            using (var session = store.OpenSession())
            {
                session.Store(subDto);
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "SubDto.Name" , ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDto.Name", ["Subname1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void DocumentsWithNestedArrayPropertyPath()
    {
        using (var store = GetDocumentStore())
        {
            var subDto1 = new SubDto() { Name = "Subname1" };
            var subDto2 = new SubDto() { Name = "Subname2" };
            var dto = new Dto { SubDtos = [subDto1, subDto2] };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "SubDtos.Name" , ChunkingOptions = DefaultChunkingOptions}]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDtos.Name", ["Subname1", "Subname2"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EmbeddingsMustBeGeneratedOnlyOnceInSameBatch()
    {
        using (var store = GetDocumentStore())
        {
            var dto1 = new Dto { Name = "Name1" };
            var dto2 = new Dto { Name = "Name1" };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(dto1);
                await session.StoreAsync(dto2);
                await session.SaveChangesAsync();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto2.Id);

            var db = await GetDatabase(store.Database);
            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();
            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(2, stats.Length);
            Assert.Equal(2, stats[0].NumberOfLoadedItems);
            Assert.Equal("No more items to process", stats[1].BatchTransformationCompleteReason);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void EmbeddingsMustBeGeneratedOnlyOnceInDifferentBatches()
    {
        using (var store = GetDocumentStore())
        {
            var dto1 = new Dto { Name = "Name1" };
            var dto2 = new Dto { Name = "Name1" };

            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

            var embeddingDocName = EmbeddingsHelper.GetEmbeddingCacheDocumentId(aiConnectionStringIdentifier, EmbeddingsHelper.CalculateInputValueHash("Name1"), VectorEmbeddingType.Single);

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            //Assert document exists
            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto1.Id);

            string expectedChangeVector;
            using (var session = store.OpenSession())
            {
                expectedChangeVector = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
            }

            aiTaskDone.Reset();
            using (var session = store.OpenSession())
            {
                session.Store(dto2);
                session.SaveChanges();
            }
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var changeVectorAfterUpdate = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
                Assert.Equal(expectedChangeVector, changeVectorAfterUpdate);
            }

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto2.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void UpdateOfDocumentsWithSingleValue()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "Name1" };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto.Id);

            aiTaskDone.Reset();
            using (var session = store.OpenSession())
            {
                var loadDoc = session.Load<Dto>(dto.Id);
                loadDoc.Name = "updated";
                session.SaveChanges();
                dto = loadDoc;
            }
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["updated"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HandlingOfNonStringValues()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Age = 21 };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Age", ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Age", ["21"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void FieldsToIncludeMustBeRespected()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Names = new List<string>() { "Name1", "Name2" }, Name = "SomeName" };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connectionString) = AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["SomeName"], dto.Id);

            using (var session = store.OpenSession())
            {
                var embeddingCacheCount = session.Advanced.RawQuery<object>("from @embeddings-cache").Count();
                Assert.Equal(1, embeddingCacheCount);
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ModificationOfNonProcessedFieldsWillTriggerTaskButWontGenerateEmbeddings()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "SomeName", Age = 21 };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                AddEmbeddingsGenerationTask(store);
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var db = await GetDatabase(store.Database);

                var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                Assert.Equal(1, stats[0].NumberOfLoadedItems);

                var loadDetails = stats[0].Details.Operations[^1];
                
                Assert.Equal("Load", loadDetails.Name);
                
                var embeddingsGenerationStats = (EmbeddingsGenerationPerformanceOperation)loadDetails.Operations.First(x => x.Name == EmbeddingsGenerationOperations.GenerateInAiService);

                Assert.Equal(1, embeddingsGenerationStats.NumberOfGeneratedEmbeddings);
                
                aiTaskDone.Reset();

                dto.Age = 37;
                session.SaveChanges();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var stats2 = etlProcess.GetPerformanceStats();
                
                // there was new task run
                
                Assert.True(stats2.Length > stats.Length, $"{stats2.Length} > {stats.Length}");
                Assert.Equal(1, stats2[^1].NumberOfLoadedItems);

                var loadDetails2 = stats2[^1].Details.Operations[^1];

                Assert.Equal("Load", loadDetails2.Name);

                var embeddingsGenerationStats2 = (EmbeddingsGenerationPerformanceOperation)loadDetails2.Operations.FirstOrDefault(x => x.Name == EmbeddingsGenerationOperations.GenerateInAiService);

                Assert.Null(embeddingsGenerationStats2); // but there was no need to generate embeddings
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task DefaultBatchSizeMustBeRespected()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            await using (BulkInsertOperation bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 10_000; i++)
                {
                    await bulkInsert.StoreAsync(new Dto { Name = "Name #" + i, });
                }
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 128");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CustomBatchSizeMustBeRespected()
    {
        const string connectionStringName = "AI Connection String Name";
        const int batchSize = 4;

        var options = new Options()
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Ai.EmbeddingsGenerationTaskMaxBatchSize)] = batchSize.ToString()
        };

        using (var store = GetDocumentStore(options))
        {
            await using (BulkInsertOperation bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                {
                    await bulkInsert.StoreAsync(new Dto { Name = "Name #" + i, });
                }
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HandlingOfDocumentDeletions()
    {
        var dto1 = new Dto { Name = "Name1" };
        var dto2 = new Dto { Name = "Name2" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();

                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                AddEmbeddingsGenerationTask(store);
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                aiTaskDone.Reset();

                session.Delete(dto1);
                session.SaveChanges();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            }

            var documentEmbeddingsId1 = EmbeddingsHelper.GetEmbeddingDocumentId(dto1.Id);
            var documentEmbeddingsId2 = EmbeddingsHelper.GetEmbeddingDocumentId(dto2.Id);

            using (var session = store.OpenSession())
            {
                var documentEmbeddings1 = session.Load<object>(documentEmbeddingsId1);
                var documentEmbeddings2 = session.Load<object>(documentEmbeddingsId2);

                Assert.Null(documentEmbeddings1);
                Assert.NotNull(documentEmbeddings2);
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void WillSetExpirationOnCacheDocuments()
    {
        var dto = new Dto { Name = "Name1" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var allDocs = session.Advanced.RawQuery<Test>("from '@all_docs' as t select { Id: id(t), Expires: t[\"@metadata\"][\"@expires\"] }").ToList();
                var embedding = allDocs.First(x => x.Id.StartsWith("embeddings-cache/"));
                Assert.NotNull(embedding);
                Assert.NotNull(embedding.Expires);

                var restOfDocs = allDocs.Where(x => x.Id != embedding.Id).Select(i => i.Expires).ToList();
                Assert.All(restOfDocs, x => Assert.Null(x));
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task WillUpdateExpirationOnCacheDocuments()
    {
        var dto = new Dto { Id = "dtos/1", Name = "Name1" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, _) = AddEmbeddingsGenerationTask(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            Test embeddingCache;

            using (var session = store.OpenSession())
            {
                var cacheDocs = session.Advanced.RawQuery<Test>("from '@embeddings-cache' as t select { Id: id(t), Expires: t[\"@metadata\"][\"@expires\"] }").ToList();

                Assert.Equal(1, cacheDocs.Count);

                embeddingCache = cacheDocs[0];
                Assert.NotNull(embeddingCache);
                Assert.NotNull(embeddingCache.Expires);
            }

            var documentDatabase = await GetDatabase(store.Database);

            var now = documentDatabase.Time.GetUtcNow();

            documentDatabase.Time.UtcDateTime = () => now.Add(config.EmbeddingsCacheExpiration / 2);

            aiTaskDone.Reset();
            using (var session = store.OpenSession())
            {
                dto.Names = new List<string>() { "abc" };
                session.Store(dto);
                session.SaveChanges();
            }

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var cacheDocs = session.Advanced.RawQuery<Test>("from '@embeddings-cache' as t select { Id: id(t), Expires: t[\"@metadata\"][\"@expires\"] }").ToList();

                Assert.Equal(1, cacheDocs.Count);

                var updatedEmbeddingCache = cacheDocs[0];
                Assert.NotNull(updatedEmbeddingCache);
                Assert.NotNull(updatedEmbeddingCache.Expires);

                Assert.True(updatedEmbeddingCache.Expires > embeddingCache.Expires, $"{updatedEmbeddingCache.Expires} > {embeddingCache.Expires}");
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void SimpleJsTransformation()
    {
        const string aiIntegrationName = "local-Onnx-AI";
        var dto = new Dto { Name = "Name1" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: aiIntegrationName, script: @"
embeddings.generate(
{
    Foo: this.Name, 
    Bar: 'ConstValue'
});");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Foo", ["Name1"], dto.Id);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Bar", ["ConstValue"], dto.Id);
        }
    }

    private record Test(string Id, DateTime? Expires);

    [RavenFact(RavenTestCategory.Ai)]
    public void TextChunkingInScript()
    {
        const string plainTextToChunk =
            "text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk";
        string[] expectedChunks = ["text to chunk", "text to chunk text", "to chunk text to", "chunk text to chunk"];

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
                script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 5) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void MarkdownChunkingInScript()
    {
        const string markdownTextToChunk =
            @"# Sample Markdown

## Heading Level 2

This is a **bold** word, and this is an *italic* word.  
Here's a [link to example.com](https://example.com).

### Unordered List
- First item
- Second item
- Third item

### Ordered List
1. First item
2. Second item
3. Third item

> This is a blockquote.

`Inline code` and a code block:

```csharp
// C# code sample
Console.WriteLine(""Hello, World!"");";
#pragma warning disable SKEXP0050
        string[] expectedChunks = TextChunker.SplitMarkDownLines(markdownTextToChunk, 20).ToArray();
#pragma warning restore SKEXP0050

        var dto = new Dto { Name = markdownTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                script: "embeddings.generate({ ChunkedName: markdown.splitLines(this.Name, 20) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CanUseHtmlChunkingInScript()
    {
        const string htmlTextToChunk =
            @"<html>
<head>
    <title>Sample HTML</title>
</head>
<body>
    <h1>Hello, <span style=""color: red;"">World!</span></h1>
    <p>This is a <strong>test</strong> paragraph with <a href=""https://example.com"">a link</a>.</p>
    <ul>
        <li>First item</li>
        <li>Second item</li>
        <li>Third item</li>
    </ul>
    <!-- This is a comment -->
</body>
</html>";
        string[] expectedChunks = ["Sample HTML Hello, World! This", " is a test paragraph with", " a link . First item", " Second item Third item"];

        var dto = new Dto { Name = htmlTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                script: "embeddings.generate({ ChunkedName: html.strip(this.Name, 5) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public void CanUseHtmlChunkingInPaths()
    {
        const string htmlTextToChunk =
            @"<html>
<head>
    <title>Sample HTML</title>
</head>
<body>
    <h1>Hello, <span style=""color: red;"">World!</span></h1>
    <p>This is a <strong>test</strong> paragraph with <a href=""https://example.com"">a link</a>.</p>
    <ul>
        <li>First item</li>
        <li>Second item</li>
        <li>Third item</li>
    </ul>
    <!-- This is a comment -->
</body>
</html>";
        string[] expectedChunks = ["Sample HTML Hello, World! This", " is a test paragraph with", " a link . First item", " Second item Third item"];

        var dto = new Dto { Name = htmlTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: new List<EmbeddingPathConfiguration>()
            {
                new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = new ChunkingOptions()
                {
                    ChunkingMethod = ChunkingMethod.HtmlStrip,
                    MaxTokensPerChunk = 5
                }}
            });

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TransformationWithArrayFieldOutput()
    {
        var dto = new Dto { Name = "CoolName" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                script: "embeddings.generate({ ArrayField: [this.Name, 'ConstValue'] });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ArrayField", ["CoolName", "ConstValue"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Vector | RavenTestCategory.Etl)]
    public void QuantizationOfEmbeddingsInTwoTasks()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "CoolName", Names = ["CoolName"] };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }
        
        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (configuration1, connectionString1) = AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }], targetQuantization: VectorEmbeddingType.Int8);
        Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
        
        AssertEmbeddingsForPath(store, configuration1, connectionString1, "Name", ["CoolName"], id, VectorEmbeddingType.Int8);
        
        aiTaskDone.Reset();
        var (configuration2, connectionString2) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "secondtask",
            embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }], targetQuantization: VectorEmbeddingType.Single);
        Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
        
        AssertEmbeddingsForPath(store, configuration1, connectionString1, "Name", ["CoolName"], id, VectorEmbeddingType.Int8);
        AssertEmbeddingsForPath(store, configuration2, connectionString2, "Names", ["CoolName"], id);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void QuantizationOfEmbeddingsInTask()
    {
        var dto = new Dto { Name = "CoolName" };
        var targetQuantization = VectorEmbeddingType.Binary;

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();

                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                    embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }], targetQuantization: targetQuantization);

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
                var integrationIdentifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

                AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, "Name", [dto.Name], dto.Id, targetQuantization: targetQuantization);

                var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(dto.Name);
                var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput, targetQuantization);

                var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
                Assert.NotNull(embeddingCacheDocument);

                var expectedAttachmentNameInEmbeddingsDocument =
                    EmbeddingsHelper.GenerateDestinationAttachmentName(EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(integrationIdentifier, "Name"),
                        hashOfInput, targetQuantization);

                var documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(dto.Id);
                var documentEmbeddings = session.Load<object>(documentEmbeddingsId) as JObject;
                Assert.NotNull(documentEmbeddings);

                using (var embeddingAttachment = session.Advanced.Attachments.Get(documentEmbeddingsId, expectedAttachmentNameInEmbeddingsDocument))
                {
                    Assert.NotNull(embeddingAttachment);

                    var buffer = new byte[48];

                    using (var attachmentStream = new MemoryStream(buffer))
                    {
                        embeddingAttachment.Stream.CopyTo(attachmentStream);

                        var embeddingValue = attachmentStream.ToArray();

                        Assert.NotEmpty(embeddingValue);
                    }
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void ChunkingInEmbeddingsGenerationTaskConfiguration()
    {
        var subDto = new SubDto() { Name = "pretty long text that will generate multiple chunks" };
        var dto = new Dto { Name = "different text that won't be chunked because of the configuration", SubDto = subDto};

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(subDto);
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                    embeddingsPaths: [
                        new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = new ChunkingOptions()
                        {
                            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
                            MaxTokensPerChunk = 2137
                        }},
                        new EmbeddingPathConfiguration() { Path = "SubDto.Name", ChunkingOptions = new ChunkingOptions()
                        {
                            ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
                            MaxTokensPerChunk = 5
                        }}
                    ]);
                
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var result = session.Query<Dto>().VectorSearch(x =>
                    x.WithText("SubDto.Name", configuration.Identifier), factory => factory.ByText("text")).ToList();

                Assert.Single(result);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai, Skip = "RavenDB-23834")]
    public async Task CanTestEmbeddingsGenerationScript()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var order = new Order
                {
                    Lines =
                    [
                        new OrderLine { ProductName = "Carbon replacement feather for raven wing", Quantity = 450 },
                        new OrderLine { ProductName = "Plasma gun mount for raven's foot (left-side)", Quantity = 1 }
                    ]
                };

                await session.StoreAsync(order);
                await session.SaveChangesAsync();
            }

            var connectionString = new AiConnectionString { Name = "ConnectionStringForTestingPurposes", EmbeddedSettings = new EmbeddedSettings() };
            var operation = new PutConnectionStringOperation<AiConnectionString>(connectionString);
            var putConnectionStringResult = store.Maintenance.Send(operation);
            Assert.NotNull(putConnectionStringResult.RaftCommandIndex);

            var configuration = new EmbeddingsGenerationConfiguration
            {
                Name = "AiIntegrationTaskForTestingPurposes",
                ConnectionStringName = "ConnectionStringForTestingPurposes",
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "Lines", ChunkingOptions = DefaultChunkingOptions }],
                Collection = "Orders",
                ChunkingOptionsForQuerying = DefaultChunkingOptions
            };

            var database = await GetDatabase(store.Database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testScript = new TestEmbeddingsGenerationScript
                {
                    DocumentId = "orders/1-A",
                    Configuration = configuration
                };

                var testResult = EmbeddingsGenerationTask.TestScript(testScript, database, database.ServerStore, context);
                Assert.NotNull(testResult);
            }
        }
    }

    internal class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<string> Names { get; set; }
        public int Age { get; set; }
        public SubDto SubDto { get; set; }
        public SubDto[] SubDtos { get; set; }
    }

    internal class SubDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
