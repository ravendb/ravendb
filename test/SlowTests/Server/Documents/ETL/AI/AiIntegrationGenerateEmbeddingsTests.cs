using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.AI;

public class AiIntegrationGenerateEmbeddingsTests(ITestOutputHelper output) : AiIntegrationTestBase(output)
{
    [RavenFact(RavenTestCategory.Etl)]
    public void CanSingleDocumentHasTwoEmbeddings()
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

        var etlDone = Etl.WaitForEtlToComplete(store);
        var (etlConfig, connection) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["Name", "SubDto.Name"]);
        etlDone.Wait(TimeSpan.FromSeconds(10));

        AssertEmbeddingsForPath(store, etlConfig, "Name", ["Name1"], id);
        AssertEmbeddingsForPath(store, etlConfig, "SubDto.Name", ["Name1"], id);

        etlDone.Reset();
        using (var session = store.OpenSession())
        {
            var dto = session.Load<Dto>(id);
            dto.Name = "Updated";
            session.Store(dto);
            session.SaveChanges();
        }

        etlDone.Wait(TimeSpan.FromSeconds(10));

        AssertEmbeddingsForPath(store, etlConfig, "Name", ["Updated"], id);
        AssertEmbeddingsForPath(store, etlConfig, "SubDto.Name", ["Name1"], id);
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["Names"]);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "Names", ["Name1", "Name2", "Name3"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["SubDto.Name"]);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "SubDto.Name", ["Subname1"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["SubDtos.Name"]);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "SubDtos.Name", ["Subname1", "Subname2"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["Name"]);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto2.Id);

            var db = await GetDatabase(store.Database);
            var etlProcess = (AiIntegration)db.EtlLoader.Processes.First();
            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(2, stats.Length);
            Assert.Equal(2, stats[0].NumberOfLoadedItems);
            Assert.Equal("No more items to process", stats[1].BatchTransformationCompleteReason);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl);
            var embeddingDocName = AiHelper.GetValueEmbeddingsDocumentId(configuration.NormalizedConnectionName, AiHelper.CalculateValueHash("Name1"));
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            //Assert document exists
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto1.Id);

            string expectedChangeVector;
            using (var session = store.OpenSession())
            {
                expectedChangeVector = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
            }
            
            etlDone.Reset();
            using (var session = store.OpenSession())
            {
                session.Store(dto2);
                session.SaveChanges();
            }
            etlDone.Wait(TimeSpan.FromSeconds(10));

            using (var session = store.OpenSession())
            {
                var changeVectorAfterUpdate = session.Advanced.GetChangeVectorFor(session.Load<object>(embeddingDocName));
                Assert.Equal(expectedChangeVector, changeVectorAfterUpdate);
            }
            
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto2.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, configuration, "Name", ["Name1"], dto.Id);
            
            etlDone.Reset();
            using (var session = store.OpenSession())
            {
                var loadDoc = session.Load<Dto>(dto.Id);
                loadDoc.Name = "updated";
                session.SaveChanges();
                dto = loadDoc;
            }
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            AssertEmbeddingsForPath(store, configuration, "Name", ["updated"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (etlConfig, _) = RegisterAiIntegration(store, Etl, embeddingsPaths: ["Age"]);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, etlConfig, "Age", ["21"], dto.Id);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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


            var etlDone = Etl.WaitForEtlToComplete(store);
            var (etlConfig, _) = RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(10));
            AssertEmbeddingsForPath(store, etlConfig, "Name", ["SomeName"], dto.Id);

            using (var session = store.OpenSession())
            {
                var embeddingCacheCount = session.Advanced.RawQuery<object>("from @embeddings").Count();
                Assert.Equal(1, embeddingCacheCount);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task TestIfModificationOfNonProcessedFieldsTriggersEtl()
    {
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "SomeName", Age = 21 };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
                var etlDone = Etl.WaitForEtlToComplete(store);
                RegisterAiIntegration(store, Etl);
                etlDone.Wait(TimeSpan.FromSeconds(10));

                var db = await GetDatabase(store.Database);

                var etlProcess = (AiIntegration)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                etlDone.Reset();

                dto.Age = 37;
                session.SaveChanges();

                etlDone.Wait(TimeSpan.FromSeconds(10));

                var etlStats2 = etlProcess.GetPerformanceStats();
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(100));

            var db = await GetDatabase(store.Database);

            var etlProcess = (AiIntegration)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 128");
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

            var etlDone = Etl.WaitForEtlToComplete(store);
            RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(10));

            var db = await GetDatabase(store.Database);

            var etlProcess = (AiIntegration)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
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

                var etlDone = Etl.WaitForEtlToComplete(store);
                RegisterAiIntegration(store, Etl);
                etlDone.Wait(TimeSpan.FromSeconds(10));

                etlDone.Reset();

                session.Delete(dto1);
                session.SaveChanges();

                etlDone.Wait(TimeSpan.FromSeconds(10));
            }

            var documentEmbeddingsId1 = AiHelper.GetDocumentEmbeddingsId(dto1.Id);
            var documentEmbeddingsId2 = AiHelper.GetDocumentEmbeddingsId(dto2.Id);

            using (var session = store.OpenSession())
            {
                var documentEmbeddings1 = session.Load<object>(documentEmbeddingsId1);
                var documentEmbeddings2 = session.Load<object>(documentEmbeddingsId2);

                Assert.Null(documentEmbeddings1);
                Assert.NotNull(documentEmbeddings2);
            }
        }
    }


    [RavenFact(RavenTestCategory.Etl)]
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


            var etlDone = Etl.WaitForEtlToComplete(store);
            RegisterAiIntegration(store, Etl);
            etlDone.Wait(TimeSpan.FromSeconds(10));

            using (var session = store.OpenSession())
            {
                var allDocs = session.Advanced.RawQuery<Test>("from '@all_docs' as t select { Id: id(t), Expires: t[\"@metadata\"][\"@expires\"] }").ToList();
                var embedding = allDocs.First(x => x.Id.StartsWith("embeddings/"));
                Assert.NotNull(embedding);
                Assert.NotNull(embedding.Expires);

                var restOfDocs = allDocs.Where(x => x.Id != embedding.Id).Select(i => i.Expires).ToList();
                Assert.All(restOfDocs, x => Assert.Null(x));
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestTransformation()
    {
        const string connectionStringName = "connection string name";
        const string aiIntegrationName = "local-Onnx-AI";
        var dto = new Dto { Name = "Name1" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var configuration = new AiIntegrationConfiguration()
            {
                Name = aiIntegrationName,
                ConnectionStringName = connectionStringName,
                Collection = "Dtos",
                EmbeddingsTransformation = new AiEmbeddingsTransformation
                {
                    Script = @"
embeddings.generate(
{
    Foo: this.Name, 
    Bar: 'ConstValue'
});"
                }
            };
            
            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var fooValueHash = AiHelper.CalculateValueHash(dto.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.NormalizedConnectionName, fooValueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                WaitForUserToContinueTheTest(store);

                var expectedFooAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Name1;

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedFooAttachmentName, attachmentNames[0].Name);
                
                var barValueHash = AiHelper.CalculateValueHash("ConstValue");
                
                valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.NormalizedConnectionName, barValueHash);
                valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var expectedBarAttachmentName = (string)((dynamic)valueEmbeddingsDocument).ConstValue;

                attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedBarAttachmentName, attachmentNames[0].Name);
                
                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                
                var attachmentNamesForFooPropertyJArray = (JArray)configurationValues.Foo;
                var attachmentNamesForFooProperty = attachmentNamesForFooPropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForFooProperty);
                Assert.Equal(AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(aiIntegrationName, "Foo") + expectedFooAttachmentName, attachmentNamesForFooProperty[0]);
                
                
                var attachmentNamesForBarPropertyJArray = (JArray)configurationValues.Bar;
                var attachmentNamesForBarProperty = attachmentNamesForBarPropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForBarProperty);
                Assert.Equal(AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(aiIntegrationName, "Bar") + expectedBarAttachmentName, attachmentNamesForBarProperty[0]);

                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                var attachmentNamesStringList = attachmentNames.Select(x => x.Name).ToList();
                
                Assert.Equal(2, attachmentNames.Length);
                Assert.NotEqual(expectedFooAttachmentName, expectedBarAttachmentName);
                Assert.Contains(AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(aiIntegrationName, "Foo") + expectedFooAttachmentName, attachmentNamesStringList);
                Assert.Contains(AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(aiIntegrationName, "Bar") + expectedBarAttachmentName, attachmentNamesStringList);
            }
        }
    }
    

    private record Test(string Id, string Expires);

#pragma warning disable SKEXP0050
    [RavenFact(RavenTestCategory.Etl)]
    public void TestChunkingInTransformation()
    {
        const string plainTextToChunk =
            "text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk";

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            var etlDone = Etl.WaitForEtlToComplete(store);

            var (configuration, _) = RegisterAiIntegration(store, Etl,
                script: "embeddings.generate({ ChunkedName: text.splitLines(this.Name, 5) });");

            etlDone.Wait(TimeSpan.FromSeconds(20));
            using (var session = store.OpenSession())
            {
                var documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var documentEmbeddings = session.Load<object>(documentEmbeddingsId);

                WaitForUserToContinueTheTest(store);

                Assert.NotNull(documentEmbeddings);

                var configurationValues = ((dynamic)documentEmbeddings)[configuration.Name];
                var attachmentNamesForChunkedNamePropertyJArray = (JArray)configurationValues.ChunkedName;
                var attachmentNamesForNameProperty = attachmentNamesForChunkedNamePropertyJArray.ToObject<string[]>();

                Assert.Equal(8, attachmentNamesForNameProperty.Length);
            }
        }
    }
#pragma warning restore SKEXP0050

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
