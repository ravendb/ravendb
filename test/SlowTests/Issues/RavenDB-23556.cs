using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Exceptions;
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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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
                FieldsToInclude = ["Names"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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

    /*
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
            
            // todo handle lack of transforms
            var configuration = new AiEtlConfiguration()
            {
                Name = "someETLConfigurationName",
                AiConnectorType = AiConnectorType.Onnx, 
                AllowEtlOnNonEncryptedChannel = true, 
                ConnectionStringName = connectionStringName,
                FieldsToInclude = ["Age"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
        }
    }
    */

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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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
                    FieldsToInclude = ["Name"],
                    Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(100));

            var db = await GetDatabase(store.Database);
            
            var etlProcess = (AiEtl)db.EtlLoader.Processes.First();

            var stats = etlProcess.GetPerformanceStats();

            Assert.Equal(stats[0].BatchTransformationCompleteReason, "Stopping the batch because it has already processed max number of extracted documents : 8192");
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
                FieldsToInclude = ["Name"],
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }]
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

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Onnx ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Ollama ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Google ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.HuggingFace ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.AzureOpenAI ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.OpenAi ])]
    public void PutAiConnectionString_WithValidConfiguration_ShouldWork(Options options, AiConnectorType aiConnectorType)
    {
        var connectionStringName = $"AI Connection String Name {aiConnectorType}";
        var connectionString = new AiConnectionString { Name = connectionStringName };

        const string expectedApiKeyValue = "someApiKey";
        const string expectedModelValue = "someModel";
        const string expectedUriValue = "https://someUri.com";
        const string expectedDeploymentName = "someDeploymentName";

        switch (aiConnectorType)
        {
            case AiConnectorType.OpenAi:
                connectionString.OpenAiSettings = new OpenAiSettings { ApiKey = expectedApiKeyValue, Model = expectedModelValue, Endpoint = expectedUriValue};
                break;
            case AiConnectorType.AzureOpenAI:
                connectionString.AzureOpenAiSettings = new AzureOpenAiSettings { ApiKey = expectedApiKeyValue, Model = expectedModelValue, Endpoint = expectedUriValue, DeploymentName = expectedDeploymentName};
                break;
            case AiConnectorType.Ollama:
                connectionString.OllamaSettings = new OllamaSettings { Model = expectedApiKeyValue, Uri = expectedUriValue };
                break;
            case AiConnectorType.Onnx:
                connectionString.OnnxSettings = new OnnxSettings();
                break;
            case AiConnectorType.Google:
                connectionString.GoogleSettings = new GoogleSettings { ApiKey = expectedApiKeyValue, Model = expectedModelValue };
                break;
            case AiConnectorType.HuggingFace:
                connectionString.HuggingFaceSettings = new HuggingFaceSettings { ApiKey = expectedApiKeyValue, Model = expectedModelValue };
                break;
        }

        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

            var aiConnectionStringsDictionary = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName, ConnectionStringType.Ai)).AiConnectionStrings;

            Assert.NotNull(aiConnectionStringsDictionary);
            Assert.Equal(1, aiConnectionStringsDictionary.Count);
            Assert.True(aiConnectionStringsDictionary.ContainsKey(connectionStringName));

            switch (aiConnectorType)
            {
                case AiConnectorType.OpenAi:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Equal(expectedApiKeyValue, aiConnectionStringsDictionary[connectionStringName].OpenAiSettings.ApiKey);
                    Assert.Equal(expectedModelValue, aiConnectionStringsDictionary[connectionStringName].OpenAiSettings.Model);
                    Assert.Equal(expectedUriValue, aiConnectionStringsDictionary[connectionStringName].OpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.AzureOpenAI:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Equal(expectedApiKeyValue, aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings.ApiKey);
                    Assert.Equal(expectedModelValue, aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings.Model);
                    Assert.Equal(expectedUriValue, aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.Ollama:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Equal(expectedApiKeyValue, aiConnectionStringsDictionary[connectionStringName].OllamaSettings.Model);
                    Assert.Equal(expectedUriValue, aiConnectionStringsDictionary[connectionStringName].OllamaSettings.Uri);
                    break;

                case AiConnectorType.Onnx:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    break;

                case AiConnectorType.Google:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);
                    Assert.Equal(expectedApiKeyValue, aiConnectionStringsDictionary[connectionStringName].GoogleSettings.ApiKey);
                    Assert.Equal(expectedModelValue, aiConnectionStringsDictionary[connectionStringName].GoogleSettings.Model);
                    break;

                case AiConnectorType.HuggingFace:
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[connectionStringName].GoogleSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings);
                    Assert.Equal(expectedApiKeyValue, aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings.ApiKey);
                    Assert.Equal(expectedModelValue, aiConnectionStringsDictionary[connectionStringName].HuggingFaceSettings.Model);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(aiConnectorType), aiConnectorType, null);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Onnx ], Skip = "OnnxSettings has no mandatory fields")]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Ollama ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.Google ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.HuggingFace ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.AzureOpenAI ])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [ AiConnectorType.OpenAi ])]
    public void PutAiConnectionString_WithInvalidConfiguration_ShouldThrow(Options options, AiConnectorType aiConnectorType)
    {
        var connectionStringName = $"AI Connection String Name {aiConnectorType}";
        var connectionString = new AiConnectionString { Name = connectionStringName };

        const string expectedApiKeyValue = "someApiKey";
        const string expectedUriValue = "https://someUri.com";
        const string expectedDeploymentName = "someDeploymentName";

        switch (aiConnectorType)
        {
            case AiConnectorType.OpenAi:
                connectionString.OpenAiSettings = new OpenAiSettings { ApiKey = expectedApiKeyValue, Endpoint = expectedUriValue};
                break;
            case AiConnectorType.AzureOpenAI:
                connectionString.AzureOpenAiSettings = new AzureOpenAiSettings { ApiKey = expectedApiKeyValue, Endpoint = expectedUriValue, DeploymentName = expectedDeploymentName};
                break;
            case AiConnectorType.Ollama:
                connectionString.OllamaSettings = new OllamaSettings { Uri = expectedUriValue };
                break;
            case AiConnectorType.Google:
                connectionString.GoogleSettings = new GoogleSettings { ApiKey = expectedApiKeyValue };
                break;
            case AiConnectorType.HuggingFace:
                connectionString.HuggingFaceSettings = new HuggingFaceSettings { ApiKey = expectedApiKeyValue };
                break;
        }

        using (var store = GetDocumentStore())
        {
            var exception = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString)));
            Assert.Contains($"Value of `{nameof(OpenAiSettings.Model)}` field cannot be empty.", exception.Message);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<string> Names { get; set; }
        public int Age { get; set; }
    }
}
