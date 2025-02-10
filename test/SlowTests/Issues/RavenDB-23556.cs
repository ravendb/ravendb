using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23556 : RavenTestBase
{
    public RavenDB_23556(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentsWithSingleValue()
    {
        const string connectionStringName = "connection string name";

        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "Name1" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Name1;

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentsWithListOfValues()
    {
        const string connectionStringName = "AI Connection String Name";
        
        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Names = new List<string> { "Name1", "Name2", "Name3" } };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Names"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var expectedAttachmentNames = new List<string>();
                
                foreach (var name in dto.Names)
                {
                    var valueHash = AiHelper.CalculateValueHash(name);
                    var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                    var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                    var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument)[name];
                    expectedAttachmentNames.Add(expectedAttachmentName);

                    var cacheAttachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                    
                    Assert.Single(cacheAttachmentNames);
                    Assert.Equal(expectedAttachmentName, cacheAttachmentNames[0].Name);
                }
                
                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Names;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Equal(3, attachmentNamesForNameProperty.Length);
                Assert.Equal(expectedAttachmentNames[0], attachmentNamesForNameProperty[0]);
                Assert.Equal(expectedAttachmentNames[1], attachmentNamesForNameProperty[1]);
                Assert.Equal(expectedAttachmentNames[2], attachmentNamesForNameProperty[2]);
                
                var attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument).Select(x => x.Name).ToList();
                
                Assert.Equal(3, attachmentNames.Count);
                Assert.Contains(expectedAttachmentNames[0], attachmentNames);
                Assert.Contains(expectedAttachmentNames[1], attachmentNames);
                Assert.Contains(expectedAttachmentNames[2], attachmentNames);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentsWithNestedPropertyPath()
    {
        const string connectionStringName = "AI Connection String Name";
        
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
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["SubDto.Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto.SubDto.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Subname1;

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForSubDtoNamePropertyJArray = (JArray)configurationValues["SubDto.Name"];
                var attachmentNamesForSubDtoNameProperty = attachmentNamesForSubDtoNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForSubDtoNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForSubDtoNameProperty[0]);
                
                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentsWithNestedArrayPropertyPath()
    {
        const string connectionStringName = "AI Connection String Name";
        
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
            
            WaitForUserToContinueTheTest(store);
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["SubDto.Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            WaitForUserToContinueTheTest(store);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestIfEmbeddingsAreGeneratedOnlyOnceInSameBatch()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            var dto1 = new Dto { Name = "Name1" };
            var dto2 = new Dto { Name = "Name1" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();
            }
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            WaitForUserToContinueTheTest(store);
            
            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto1.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Name1;

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto1.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestIfEmbeddingsAreGeneratedOnlyOnceInDifferentBatches()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            var dto1 = new Dto { Name = "Name1" };
            var dto2 = new Dto { Name = "Name1" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.SaveChanges();
            }
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            string expectedAttachmentName, expectedValueEmbeddingsDocumentId, expectedChangeVector;
            
            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto1.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Name1;
                expectedValueEmbeddingsDocumentId = (string)((dynamic)valueEmbeddingsDocument).Id;
                expectedChangeVector = session.Advanced.GetChangeVectorFor(valueEmbeddingsDocument);

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto1.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
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
                var valueHash = AiHelper.CalculateValueHash(dto2.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var changeVector = session.Advanced.GetChangeVectorFor(valueEmbeddingsDocument);
                
                Assert.Equal(expectedValueEmbeddingsDocumentId, (string)((dynamic)valueEmbeddingsDocument).Id);
                Assert.Equal(expectedChangeVector, changeVector);
                Assert.Equal(expectedAttachmentName, (string)((dynamic)valueEmbeddingsDocument).Name1);
                
                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto2.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                var attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentsWithSingleValueWithUpdate()
    {
        const string connectionStringName = "connection string name";

        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "Name1" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            AssertEmbeddings();

            using (var session = store.OpenSession())
            {
                var loadDoc = session.Load<Dto>(dto.Id);
                loadDoc.Name = "updated";
                session.SaveChanges();
                dto = loadDoc;
            }

            WaitForUserToContinueTheTest(store);
            //Assert embedding after update:
            AssertEmbeddings();
            
            void AssertEmbeddings()
            {
                using (var session = store.OpenSession())
                {
                    var valueHash = AiHelper.CalculateValueHash(dto.Name);
                    var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                    var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                    //Assert.NotNull(valueEmbeddingsDocument);
                    var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).Name1;

                    var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                    Assert.Single(attachmentNames);
                    Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                    var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                    var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                    var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                    var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                    var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                    Assert.Single(attachmentNamesForNameProperty);
                    Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                    attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                    Assert.Single(attachmentNames);
                    Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestHandlingOfNonStringValues()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Age = 21 };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Age"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            WaitForUserToContinueTheTest(store);

            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto.Age.ToString());
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument)[dto.Age.ToString()];
                
                Assert.NotNull(valueEmbeddingsDocument);
                
                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
                
                var documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var documentEmbeddings = session.Load<object>(documentEmbeddingsId);
                
                Assert.NotNull(documentEmbeddings);
                
                attachmentNames = session.Advanced.Attachments.GetNames(documentEmbeddings);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestIfFieldsToIncludeAreRespected()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Names = new List<string>() { "Name1", "Name2" }, Name = "SomeName" };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }

            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var valueHash = AiHelper.CalculateValueHash(dto.Name);
                var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, valueHash);
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);
                
                var expectedAttachmentName = (string)((dynamic)valueEmbeddingsDocument).SomeName;

                var attachmentNames = session.Advanced.Attachments.GetNames(valueEmbeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);

                var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var embeddingsDocument = session.Load<object>(embeddingsDocumentId);
                
                var configurationValues = ((dynamic)embeddingsDocument)[configuration.Name];
                var attachmentNamesForNamePropertyJArray = (JArray)configurationValues.Name;
                var attachmentNamesForNameProperty = attachmentNamesForNamePropertyJArray.ToObject<string[]>();
                
                Assert.Single(attachmentNamesForNameProperty);
                Assert.Equal(expectedAttachmentName, attachmentNamesForNameProperty[0]);
                
                attachmentNames = session.Advanced.Attachments.GetNames(embeddingsDocument);
                
                Assert.Single(attachmentNames);
                Assert.Equal(expectedAttachmentName, attachmentNames[0].Name);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task TestIfModificationOfNonProcessedFieldsTriggersEtl()
    {
        const string connectionStringName = "AI Connection String Name";

        using (var store = GetDocumentStore())
        {
            var dto = new Dto { Name = "SomeName", Age = 21 };

            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();

                // todo handle lack of transforms
                var configuration = new AiEtlConfiguration()
                {
                    Name = "someETLConfigurationName",
                    AiConnectorType = AiConnectorType.Onnx,
                    AllowEtlOnNonEncryptedChannel = true,
                    ConnectionStringName = connectionStringName,
                    PathsToProcess = ["Name"],
                    Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
                };

                var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

                var etlDone = Etl.WaitForEtlToComplete(store);

                Etl.AddEtl(store, configuration, connectionString);

                etlDone.Wait(TimeSpan.FromSeconds(10));

                var db = await GetDatabase(store.Database);
            
                var etlProcess = (AiEtl)db.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();
            
                etlDone.Reset();
                
                dto.Age = 37;
                session.SaveChanges();
                
                etlDone.Wait(TimeSpan.FromSeconds(10));
                
                var etlStats2 = etlProcess.GetPerformanceStats();

                var x = 0;
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
                    await bulkInsert.StoreAsync(new Dto
                    {
                        Name = "Name #" + i,
                    });
                }
            }
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(100));

            var db = await GetDatabase(store.Database);
            
            var etlProcess = (AiEtl)db.EtlLoader.Processes.First();

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

            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));

            var db = await GetDatabase(store.Database);

            var etlProcess = (AiEtl)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 4");
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestDocumentDeletes()
    {
        const string connectionStringName = "connection string name";

        var dto1 = new Dto { Name = "Name1" };
        var dto2 = new Dto { Name = "Name2" };
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();
                
                var configuration = new AiEtlConfiguration()
                {
                    Name = "someETLConfigurationName",
                    AiConnectorType = AiConnectorType.Onnx,
                    AllowEtlOnNonEncryptedChannel = true,
                    ConnectionStringName = connectionStringName,
                    PathsToProcess = ["Name"],
                    Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
                };

                var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

                var etlDone = Etl.WaitForEtlToComplete(store);

                Etl.AddEtl(store, configuration, connectionString);

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
        const string connectionStringName = "connection string name";

        var dto = new Dto { Name = "Name1" };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));

            using (var session = store.OpenSession())
            {
                var documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var documentEmbeddings = session.Load<object>(documentEmbeddingsId);
                
                var metadata = session.Advanced.GetMetadataFor(documentEmbeddings);

                var expiration = DateTime.Parse((string)metadata[Constants.Documents.Metadata.Expires]);
                
                Assert.True(expiration > DateTime.UtcNow.AddMonths(2).AddDays(27));
            }
        }
    }
    
#pragma warning disable SKEXP0050
    [RavenFact(RavenTestCategory.Etl)]
    public void TestChunkingInTransformation()
    {
        const string connectionStringName = "connection string name";
        
        const string plainTextToChunk = "text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk text to chunk";

        var dto = new Dto { Name = plainTextToChunk };

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                PathsToProcess = ["ChunkedName"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "this.ChunkedName = splitPlainTextLines(this.Name, 5);" }]
            };
            
            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(dto.Id);
                var documentEmbeddings = session.Load<object>(documentEmbeddingsId);
                
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
