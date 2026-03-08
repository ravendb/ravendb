using System;
using System.Collections.Generic;
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
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class GenerateEmbeddingsTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CanSingleDocumentHaveTwoEmbeddings()
    {
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Name1", Names = ["Name2", "Name3"], SubDto = new SubDto { Name = "Name1" } };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths:
        [
            new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions },
            new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions },
            new EmbeddingPathConfiguration() { Path = "SubDto.Name", ChunkingOptions = DefaultChunkingOptions }
        ]);
        
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        
        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Name2", "Name3"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "SubDto.Name", ["Name1"], id);

        aiTaskDone.Reset();
        using (var session = store.OpenSession())
        {
            var dto = session.Load<Dto>(id);
            dto.Name = "Updated";
            dto.Names = ["Name2", "Name4"];
            session.Store(dto);
            session.SaveChanges();
        }

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Name2", "Name4"], id);
        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "SubDto.Name", ["Name1"], id);
        
        AssertMissingEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Name3"], id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task DocumentsWithSingleValue()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Name1"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task DocumentsWithListOfValues()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Names", ["Name1", "Name2", "Name3"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task DocumentsWithNestedPropertyPath()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "SubDto.Name", ["Subname1"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task DocumentsWithNestedArrayPropertyPath()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, config, connectionString, "SubDtos.Name", ["Subname1", "Subname2"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task EmbeddingsMustBeGeneratedOnlyOnceInDifferentBatches()
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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var changeVectorAfterUpdate = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
                Assert.Equal(expectedChangeVector, changeVectorAfterUpdate);
            }

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Name1"], dto2.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task UpdateOfDocumentsWithSingleValue()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["updated"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task HandlingOfNonStringValues()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Age", ["21"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task FieldsToIncludeMustBeRespected()
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["SomeName"], dto.Id);

            using (var session = store.OpenSession())
            {
                var embeddingCacheCount = session.Advanced.RawQuery<object>("from @embeddings-cache").Count();
                Assert.Equal(1, embeddingCacheCount);
            }
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task ModificationOfNonProcessedFieldsWillTriggerTaskButWontGenerateEmbeddings()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "SomeName", Age = 21 };
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = AddEmbeddingsGenerationTask(store);

            using (var session = store.OpenSession())
            {
                aiTaskDone.Reset();
                session.Store(dto);
                session.SaveChanges();
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                var db = await GetDatabase(store.Database);

                var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats()
                    .Where(x=>x.NumberOfLoadedItems > 0)
                    .ToArray();

                Assert.Equal(1, stats[0].NumberOfLoadedItems);

                var loadDetails = stats[0].Details.Operations[^1];
                
                Assert.Equal("Load", loadDetails.Name);
                
                var embeddingsGenerationStats = (EmbeddingsGenerationPerformanceOperation)loadDetails.Operations.First(x => x.Name == EmbeddingsGenerationOperations.GenerateInAiService);

                Assert.Equal(1, embeddingsGenerationStats.NumberOfGeneratedEmbeddings);
                
                aiTaskDone.Reset();

                dto.Age = 37;
                session.SaveChanges();

                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

                var stats2 = etlProcess.GetPerformanceStats()
                    .Where(x => x.NumberOfLoadedItems > 0)
                    .ToArray();
                
                // there was new task run
                
                Assert.True(stats2.Length > stats.Length, $"{stats2.Length} > {stats.Length}");
                Assert.Equal(1, stats2[^1].NumberOfLoadedItems);

                var loadDetails2 = stats2[^1].Details.Operations[^1];

                Assert.Equal("Load", loadDetails2.Name);

                var embeddingsGenerationStats2 = (EmbeddingsGenerationPerformanceOperation)loadDetails2.Operations.First(x => x.Name == EmbeddingsGenerationOperations.GenerateInAiService);
                // but there was no need to generate embeddings
                Assert.Equal(embeddingsGenerationStats2.NumberOfGeneratedEmbeddings, embeddingsGenerationStats2.NumberOfEmbeddingsInCache);
            }
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task DefaultBatchSizeMustBeRespected()
    {
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
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 128");
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CustomBatchSizeMustBeRespected()
    {
        const int batchSize = 4;

        var options = new Options()
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Ai.EmbeddingsGenerationMaxBatchSize)] = batchSize.ToString()
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
            var (configuration, _) = AddEmbeddingsGenerationTask(store);
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            var db = await GetDatabase(store.Database);

            var etlProcess = (EmbeddingsGenerationTask)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task HandlingOfDocumentDeletions()
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
                var (configuration, _) = AddEmbeddingsGenerationTask(store);
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                aiTaskDone.Reset();

                session.Delete(dto1);
                session.SaveChanges();
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
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

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task WillSetExpirationOnCacheDocuments()
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
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store);
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, configuration, connectionString, "Name", ["Name1"], dto.Id);
            
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

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
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
            var (config, connectionString) = AddEmbeddingsGenerationTask(store);
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Name1"], dto.Id);

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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Name1"], dto.Id);

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

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task SimpleJsTransformation()
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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Foo", ["Name1"], dto.Id);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Bar", ["ConstValue"], dto.Id);
        }
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task NestedJsTransformation()
    {
        const string aiIntegrationName = "local-Onnx-AI";
        var dto = new Dto { Name = "<h1>Name1</a1>" };

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
    Foo: [html.strip(this.Name, 15), 'hello'], 
});");

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            // testing case-insensitive hashing of strings as well here
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Foo", ["Name1", "HELLO"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task HashingOfTextShouldBeCaseInsensitiveAndTrimWhitespaces()
    {
        var dto = new Dto() { Name = "\n UPPERCASEVALUE\n\r " };
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store);

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["uppercasevalue"], dto.Id);
        }
    }

    private record Test(string Id, DateTime? Expires);

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task TextChunkingInScript()
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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void ChunkPlainTextShouldWork()
    {
        const string plainTextToChunk = "this is a relatively long text that should produce multiple chunks because of the chunking configuration (max tokens per chunk)";
        var expectedChunks = new List <string>() { "this is a relatively long text that should", " produce multiple chunks because of the chunking", " configuration (max tokens per chunk)" };
        
        var chunks = Raven.Server.Documents.AI.TextChunker.ChunkPlainText(plainTextToChunk, 8);
        
        Assert.Equal(expectedChunks, chunks);
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void PlainTextSplitLinesShouldWork()
    {
        const string plainTextToChunk = "This is a relatively - long text\n that should produce multiple chunks\n\r because of the chunking configuration; (max tokens per chunk). It also contains, separators in random places.";
        var expectedChunks = new List <string>() { "This is a relatively - long text", "that should produce multiple chunks", "because of the", "chunking configuration;", "(max tokens per chunk).", "It also contains,", "separators in random places." };
        
        var chunks = Raven.Server.Documents.AI.TextChunker.Chunk(plainTextToChunk, new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 8 });

        Assert.Equal(expectedChunks, chunks);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task MarkdownChunkingInScript()
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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CanUseHtmlChunkingInScript()
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
        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(htmlTextToChunk, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.HtmlStrip,
            MaxTokensPerChunk = 5
        }).ToArray();

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
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);
        }
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CanUseHtmlChunkingInPaths()
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
        var expectedChunks = Raven.Server.Documents.AI.TextChunker.Chunk(htmlTextToChunk, new ChunkingOptions
        {
            ChunkingMethod = ChunkingMethod.HtmlStrip,
            MaxTokensPerChunk = 5
        }).ToArray();

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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", expectedChunks, dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task TransformationWithArrayFieldOutput()
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

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "ArrayField", ["CoolName", "ConstValue"], dto.Id);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai | RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task QuantizationOfEmbeddingsInTwoTasks()
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
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration1);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, configuration1, connectionString1, "Name", ["CoolName"], id, VectorEmbeddingType.Int8);
        
        aiTaskDone.Reset();
        var (configuration2, connectionString2) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "secondtask",
            embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }], targetQuantization: VectorEmbeddingType.Single);
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration2);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, configuration1, connectionString1, "Name", ["CoolName"], id, VectorEmbeddingType.Int8);
        AssertEmbeddingsForPath(store, configuration2, connectionString2, "Names", ["CoolName"], id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task QuantizationOfEmbeddingsInTask()
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

                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
                var integrationIdentifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

                AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, "Name", [dto.Name], dto.Id, targetQuantization: targetQuantization);

                var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(dto.Name);
                var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput, targetQuantization);

                var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
                Assert.NotNull(embeddingCacheDocument);

                var documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(dto.Id);
                var documentEmbeddings = session.Load<dynamic>(documentEmbeddingsId);
                Assert.NotNull(documentEmbeddings);
                string attachmentName = documentEmbeddings.localaitask.Name[0];

                using (var embeddingAttachment = session.Advanced.Attachments.Get(documentEmbeddingsId, attachmentName))
                {
                    Assert.NotNull(embeddingAttachment);
                    Assert.Equal(48, embeddingAttachment.Details.Size);
                }
            }
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [InlineData(VectorEmbeddingType.Int8)]
    [InlineData(VectorEmbeddingType.Binary)]
    [InlineData(VectorEmbeddingType.Single)]
    public async Task ChunkingInEmbeddingsGenerationTaskConfiguration(VectorEmbeddingType quantization)
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

                var nameConfig = new EmbeddingPathConfiguration()
                {
                    Path = "Name", ChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 100 }
                };

                var subdtoNameConfig = new EmbeddingPathConfiguration()
                {
                    Path = "SubDto.Name", ChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 5 }
                };
                
                var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                    targetQuantization: quantization,
                    embeddingsPaths: [nameConfig, subdtoNameConfig]);
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                AssertEmbeddingsForPath(store, configuration, connectionString, nameConfig.Path, Raven.Server.Documents.AI.TextChunker.Chunk(dto.Name, nameConfig.ChunkingOptions).ToArray(), dto.Id, quantization);
                AssertEmbeddingsForPath(store, configuration, connectionString, subdtoNameConfig.Path, Raven.Server.Documents.AI.TextChunker.Chunk(dto.SubDto.Name, subdtoNameConfig.ChunkingOptions).ToArray(), dto.Id, quantization);

                var result = session.Query<Dto>().VectorSearch(x =>
                        x.WithText("SubDto.Name").UsingTask(configuration.Identifier).TargetQuantization(quantization),
                    factory => factory.ByText("text")).ToList();

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
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CollisionOfTwoEmbeddingsInSingleTask()
    {
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Name1", Names = ["Name1"]};
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }        
        
        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "v1", embeddingsPaths:
        [
            new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions },
            new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions },
        ]);
        
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, config, connection, "Name", ["Name1"], id);
        AssertEmbeddingsForPath(store, config, connection, "Names", ["Name1"], id);

        aiTaskDone.Reset();
        using (var session = store.OpenSession())
        {
            session.Load<Dto>(id).Name = "Updated";
            session.SaveChanges();
        }        
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        AssertEmbeddingsForPath(store, config, connection, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, config, connection, "Names", ["Name1"], id);
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CollisionOfTwoEmbeddingsInTwoTasks()
    {
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Name1", Names = ["Name1"]};
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }        
        
        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "v1", embeddingsPaths:
        [
            new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions },
        ]);
        
        var (config2, connection2) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "v2", embeddingsPaths:
        [
            new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions },
        ]);
        
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config2);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        
        AssertEmbeddingsForPath(store, config, connection, "Name", ["Name1"], id);
        AssertEmbeddingsForPath(store, config2, connection2, "Names", ["Name1"], id);

        aiTaskDone.Reset();
        using (var session = store.OpenSession())
        {
            session.Load<Dto>(id).Name = "Updated";
            session.SaveChanges();
        }        

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

        AssertEmbeddingsForPath(store, config, connection, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, config2, connection2, "Names", ["Name1"], id);
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task CanUseProjectedFieldNameForQuery()
    {
        const string plainTextToChunk = "some text that should produce a single chunk";
        string[] expectedChunks = ["some text that should produce a single chunk"];

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();

                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                    script: "embeddings.generate({ ChunkedName: text.split(this.Name, 2048) });");

                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(configuration.Identifier),
                    new AiConnectionStringIdentifier(connectionString.Identifier), "ChunkedName", expectedChunks, dto.Id);

                var result = session.Query<Dto>().Customize(x => x.WaitForNonStaleResults()).VectorSearch(x => x.WithText("ChunkedName").UsingTask(configuration.Identifier), factory => factory.ByText("something")).ToList();
                
                Assert.Single(result);
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
