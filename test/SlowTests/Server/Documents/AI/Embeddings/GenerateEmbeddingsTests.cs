using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Text;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Config;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
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
        var (config, connection) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }, new EmbeddingPathConfiguration() { Path = "SubDto.Name", ChunkingOptions = DefaultChunkingOptions }]);
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
    public void TestDocumentsWithSingleValue()
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
            var (config, connectionString) = RegisterAiIntegration(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Name1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestDocumentsWithListOfValues()
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Names", ["Name1", "Name2", "Name3"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestDocumentsWithNestedPropertyPath()
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "SubDto.Name" , ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDto.Name", ["Subname1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestDocumentsWithNestedArrayPropertyPath()
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "SubDtos.Name" , ChunkingOptions = DefaultChunkingOptions}]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDtos.Name", ["Subname1", "Subname2"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TestIfEmbeddingsAreGeneratedOnlyOnceInSameBatch()
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }]);
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
    public void TestIfEmbeddingsAreGeneratedOnlyOnceInDifferentBatches()
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
            var (config, connectionString) = RegisterAiIntegration(store);
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
    public void TestDocumentsWithSingleValueWithUpdate()
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
            var (config, connectionString) = RegisterAiIntegration(store);
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
    public void TestHandlingOfNonStringValues()
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Age", ChunkingOptions = DefaultChunkingOptions }]);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Age", ["21"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestIfFieldsToIncludeAreRespected()
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
            var (config, connectionString) = RegisterAiIntegration(store);
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
    public async Task TestIfModificationOfNonProcessedFieldsTriggersEtl()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "SomeName", Age = 21 };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                RegisterAiIntegration(store);
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var db = await GetDatabase(store.Database);

                var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                aiTaskDone.Reset();

                dto.Age = 37;
                session.SaveChanges();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var etlStats2 = etlProcess.GetPerformanceStats();
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TestIfDefaultBatchSizeIsRespected()
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
            var (configuration, _) = RegisterAiIntegration(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 128");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TestIfCustomBatchSizeIsRespected()
    {
        const string connectionStringName = "AI Connection String Name";
        const int batchSize = 4;

        var options = new Options()
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Ai.MaxNumberOfExtractedDocuments)] = batchSize.ToString()
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
            RegisterAiIntegration(store);
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestDocumentDeletes()
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
                RegisterAiIntegration(store);
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
    public void TestDocumentExpiration()
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
            RegisterAiIntegration(store);
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
    public void TestTransformation()
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

            var (configuration, connectionString) = RegisterAiIntegration(store, embeddingsGenerationTaskName: aiIntegrationName, script: @"
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

    private record Test(string Id, string Expires);

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

            var (configuration, connectionString) = RegisterAiIntegration(store,
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

            var (configuration, connectionString) = RegisterAiIntegration(store,
                script: "embeddings.generate({ ChunkedName: markdown.splitLines(this.Name, 20) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HtmlChunkingInScript()
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
        string[] expectedChunks = ["Sample HTML", "Hello, World!", "This is a test", "paragraph with a link.", "First item", "Second item", "Third item"];

        var dto = new Dto { Name = htmlTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = RegisterAiIntegration(store,
                script: "embeddings.generate({ ChunkedName: html.splitLines(this.Name, 5) });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestTransformationWithArrayFieldOutput()
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

            var (configuration, connectionString) = RegisterAiIntegration(store,
                script: "embeddings.generate({ ArrayField: [this.Name, 'ConstValue'] });");

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ArrayField", ["CoolName", "ConstValue"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TestQuantizationOfEmbeddingsInTask()
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

                var (configuration, connectionString) = RegisterAiIntegration(store,
                    embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }], targetQuantization: targetQuantization);

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
                var integrationIdentifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

                AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, "Name", [dto.Name], dto.Id, targetQuantization: targetQuantization);

                var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(dto.Name);
                var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput, targetQuantization);

                var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
                Assert.NotNull(embeddingCacheDocument);

                var expectedAttachmentNameInEmbeddingsDocument = EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(integrationIdentifier, "Name") + hashOfInput;

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
    public void TestChunkingInAiTaskConfiguration()
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

                var (configuration, connectionString) = RegisterAiIntegration(store,
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
                
                WaitForUserToContinueTheTest(store);
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
