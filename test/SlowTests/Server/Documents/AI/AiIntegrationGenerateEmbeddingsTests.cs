using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Config;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI;

public class AiIntegrationGenerateEmbeddingsTests(ITestOutputHelper output) : AiIntegrationTestBase(output)
{
    [RavenFact(RavenTestCategory.AiIntegration)]
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
        var (config, connection) = RegisterAiIntegration(store, embeddingsPaths: ["Name", "SubDto.Name"]);
        aiTaskDone.Wait(TimeSpan.FromSeconds(10));

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

        aiTaskDone.Wait(TimeSpan.FromSeconds(10));
        
        WaitForUserToContinueTheTest(store);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "SubDto.Name", ["Name1"], id);
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Name1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["Names"]);
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Names", ["Name1", "Name2", "Name3"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["SubDto.Name"]);
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDto.Name", ["Subname1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["SubDtos.Name"]);
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDtos.Name", ["Subname1", "Subname2"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["Name"]);
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

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

    [RavenFact(RavenTestCategory.AiIntegration)]
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

            var embeddingDocName = EmbeddingsHelper.GetEmbeddingCacheDocumentId(aiConnectionStringIdentifier, EmbeddingsHelper.CalculateInputValueHash("Name1"));

            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

            using (var session = store.OpenSession())
            {
                var changeVectorAfterUpdate = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
                Assert.Equal(expectedChangeVector, changeVectorAfterUpdate);
            }

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto2.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["updated"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["Age"]);
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Age", ["21"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["SomeName"], dto.Id);

            using (var session = store.OpenSession())
            {
                var embeddingCacheCount = session.Advanced.RawQuery<object>("from @embeddings-cache").Count();
                Assert.Equal(1, embeddingCacheCount);
            }
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
                aiTaskDone.Wait(TimeSpan.FromSeconds(10));

                var db = await GetDatabase(store.Database);

                var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                aiTaskDone.Reset();

                dto.Age = 37;
                session.SaveChanges();

                aiTaskDone.Wait(TimeSpan.FromSeconds(10));

                var etlStats2 = etlProcess.GetPerformanceStats();
            }
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(100));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 128");
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
                aiTaskDone.Wait(TimeSpan.FromSeconds(10));

                aiTaskDone.Reset();

                session.Delete(dto1);
                session.SaveChanges();

                aiTaskDone.Wait(TimeSpan.FromSeconds(10));
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


    [RavenFact(RavenTestCategory.AiIntegration)]
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
            aiTaskDone.Wait(TimeSpan.FromSeconds(10));

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

    [RavenFact(RavenTestCategory.AiIntegration)]
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

            var (configuration, connectionString) = RegisterAiIntegration(store, aiIntegrationName: aiIntegrationName, script: @"
embeddings.generate(
{
    Foo: this.Name, 
    Bar: 'ConstValue'
});");

            aiTaskDone.Wait(TimeSpan.FromSeconds(10));
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Foo", ["Name1"], dto.Id);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Bar", ["ConstValue"], dto.Id);
        }
    }
    
    private record Test(string Id, string Expires);
    
    [RavenFact(RavenTestCategory.AiIntegration)]
    public void TestChunkingInTransformation()
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
            
            aiTaskDone.Wait(TimeSpan.FromSeconds(20));
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
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
            
            aiTaskDone.Wait(TimeSpan.FromSeconds(20));
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ArrayField", ["CoolName", "ConstValue"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
    public void TestQuantizationOfEmbeddingsInTask()
    {
        var dto = new Dto { Name = "CoolName" };
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                var (configuration, connectionString) = RegisterAiIntegration(store,
                    embeddingsPaths: ["Name"], targetQuantization: VectorEmbeddingType.Binary);

                aiTaskDone.Wait(TimeSpan.FromSeconds(20));

                var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
                var integrationIdentifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

                AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, "Name", [dto.Name], dto.Id);
                
                var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(dto.Name);
                var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput);
                
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

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<string> Names { get; set; }
        public int Age { get; set; }
        public SubDto SubDto { get; set; }
        public SubDto[] SubDtos { get; set; }
    }

    private class SubDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
