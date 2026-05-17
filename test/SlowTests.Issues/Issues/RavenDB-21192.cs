using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Raven.Server.Utils.Monitoring;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_21192 : RavenTestBase
{
    public RavenDB_21192(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestTaskErrorsStorage()
    {
        const string connectionStringName1 = "ConnectionString1";
        const string etlName1 = "ETL1";
        const string transformationName1 = "Transformation1";
        const string script1 = """
                               if (this.Name == "James Doe")
                               {
                                    throw new Error("dummy error");
                               }                   
                               loadToUsers(this);
                               """;
        var collections1 = new List<string>() { "Users" };
        
        const string etlName2 = "ETL2";
        const string transformationName2 = "Transformation2";

        var processName1 = EtlProcess.GetProcessName(etlName1, transformationName1);
        var processName2 = EtlProcess.GetProcessName(etlName2, transformationName2);
        
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            AddEtlTask(src, dest, etlName2, connectionStringName1, [transformationName2], [script1], collections1);

            var now = DateTime.UtcNow;

            var error1 = new TaskProcessError
            {
                CreatedAt = now,
                TaskName = processName1,
                AffectedDocumentsCount = 1,
                Step = TaskErrorStep.Transformation,
                Error = "Test message"
            };

            var error2 = new TaskProcessError
            {
                CreatedAt = now.AddDays(1),
                TaskName = processName2,
                AffectedDocumentsCount = 21,
                Step = TaskErrorStep.Load,
                Error = "Test message"
            };
                
            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, error1);
            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, error2);

            var errors = database.TaskErrorsStorage.ReadAllProcessErrors(TaskCategory.Etl);
            
            Assert.Equal(2, errors.Count);
            
            Assert.Equal(error1.CreatedAt, errors[0].CreatedAt);
            Assert.Equal(error1.TaskName, errors[0].TaskName);
            Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
            Assert.Equal((long)error1.Step, errors[0].Step);
            Assert.Equal(error1.Error, errors[0].Error);

            Assert.Equal(error2.CreatedAt, errors[1].CreatedAt);
            Assert.Equal(error2.TaskName, errors[1].TaskName);
            Assert.Equal(error2.AffectedDocumentsCount, errors[1].AffectedDocumentsCount);
            Assert.Equal((long)error2.Step, errors[1].Step);
            Assert.Equal(error2.Error, errors[1].Error);

            var itemError1 = new TaskItemError
            {
                DocumentId = "doc/1", 
                TaskName = processName1,
                CreatedAt = now,
                Step = TaskErrorStep.Load,
                Error = "Item error"
            };
            
            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.Etl, processName1, [itemError1]);

            var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);

            Assert.Single(itemErrors);
                
            Assert.Equal(itemError1.CreatedAt, itemErrors[0].CreatedAt);
            Assert.Equal(itemError1.TaskName, itemErrors[0].TaskName);
            Assert.Equal(itemError1.DocumentId, itemErrors[0].DocumentId);
            Assert.Equal((long)itemError1.Step, itemErrors[0].Step);
            Assert.Equal(itemError1.Error, itemErrors[0].Error);

            var processErrors = database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.Etl,processName1);
                
            Assert.Single(processErrors);

            Assert.Equal(error1.CreatedAt, processErrors[0].CreatedAt);
            Assert.Equal(error1.TaskName, processErrors[0].TaskName);
            Assert.Equal(error1.AffectedDocumentsCount, processErrors[0].AffectedDocumentsCount);
            Assert.Equal((long)error1.Step, processErrors[0].Step);
            Assert.Equal(error1.Error, processErrors[0].Error);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void DeleteErrorsOfTask_ShouldRemoveProcessAndItemErrors()
    {
        const string etlName = "ETL1";
        const string transformationName = "Transformation1";
        var processName = EtlProcess.GetProcessName(etlName, transformationName);

        using (var src = GetDocumentStore())
        {
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();

            var now = DateTime.UtcNow;

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = processName,
                AffectedDocumentsCount = 1,
                Step = TaskErrorStep.Transformation,
                Error = "process error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.Etl, processName,
            [
                new TaskItemError
                {
                    DocumentId = "users/1",
                    TaskName = processName,
                    CreatedAt = now,
                    Step = TaskErrorStep.Transformation,
                    Error = "item error"
                }
            ]);

            Assert.Single(database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.Etl, processName));
            Assert.Single(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl, processName));

            database.TaskErrorsStorage.DeleteErrorsOfTask(processName, TaskCategory.Etl);

            Assert.Empty(database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.Etl, processName));
            Assert.Empty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl, processName));
        }
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    public async Task FooterStatistics_ShouldReturnSeparateCountsForEtlAndAiTasksErrors()
    {
        const string etlConnectionStringName = "EtlConnectionString";
        const string etlName = "ETL1";
        const string etlTransformationName = "Transformation1";
        const string etlScript = "loadToUsers(this);";
        var etlCollections = new List<string> { "Users" };

        const string aiConnectionStringName = "AiConnectionString";
        const string aiTaskName = "EmbeddingsTask1";

        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            AddEtlTask(src, dest, etlName, etlConnectionStringName, [etlTransformationName], [etlScript], etlCollections);
            
            var aiConnectionString = new AiConnectionString
            {
                Name = aiConnectionStringName,
                ModelType = AiModelType.TextEmbeddings,
                EmbeddedSettings = new EmbeddedSettings()
            };
            aiConnectionString.Identifier = aiConnectionString.GenerateIdentifier();
            src.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(aiConnectionString));

            var aiConfiguration = new EmbeddingsGenerationConfiguration
            {
                Name = aiTaskName,
                ConnectionStringName = aiConnectionStringName,
                Collection = "Users",
                EmbeddingsTransformation = new EmbeddingsTransformation
                {
                    Script = "embeddings.generate({ Name: this.Name });",
                    ChunkingOptions = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
                },
                ChunkingOptionsForQuerying = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
            };
            aiConfiguration.Identifier = aiConfiguration.GenerateIdentifier();
            src.Maintenance.Send(new AddEmbeddingsGenerationOperation(aiConfiguration));

            var database = await GetDatabase(src.Database);

            var etlProcessName = EtlProcess.GetProcessName(etlName, etlTransformationName);
            var aiProcessName = EtlProcess.GetProcessName(aiTaskName, "embeddings-transform-script");

            var now = DateTime.UtcNow;
            
            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = etlProcessName,
                AffectedDocumentsCount = 1,
                Step = TaskErrorStep.Transformation,
                Error = "ETL transformation error"
            });

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
            {
                CreatedAt = now.AddSeconds(1),
                TaskName = etlProcessName,
                AffectedDocumentsCount = 5,
                Step = TaskErrorStep.Load,
                Error = "ETL load error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.Etl, etlProcessName, [new TaskItemError
            {
                DocumentId = "users/1",
                TaskName = etlProcessName,
                CreatedAt = now,
                Step = TaskErrorStep.Transformation,
                Error = "ETL item error"
            }]);
            
            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Ai, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = aiProcessName,
                AffectedDocumentsCount = 3,
                Step = TaskErrorStep.Transformation,
                Error = "AI transformation error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.Ai, aiProcessName, [
                new TaskItemError
                {
                    DocumentId = "users/2",
                    TaskName = aiProcessName,
                    CreatedAt = now,
                    Step = TaskErrorStep.Transformation,
                    Error = "AI item error 1"
                },
                new TaskItemError
                {
                    DocumentId = "users/3",
                    TaskName = aiProcessName,
                    CreatedAt = now.AddSeconds(1),
                    Step = TaskErrorStep.Transformation,
                    Error = "AI item error 2"
                }
            ]);
            
            var stats = src.Maintenance.Send(new GetStudioFooterStatisticsOperation());

            Assert.NotNull(stats);
            Assert.Equal(3, stats.CountOfEtlTasksErrors);
            Assert.Equal(3, stats.CountOfAiTasksErrors);
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task HealthStatusShouldBecomeFailedOnScriptErrorsAndRecoverAfterFix()
    {
        const string connectionStringName = "ConnectionString1";
        const string etlName = "ETL1";
        const string transformationName = "Transformation1";

        const string goodScript = "loadToUsers(this);";
        const string badScript = """
                                 throw new Error("always fails");
                                 loadToUsers(this);
                                 """;

        var collections = new List<string> { "Users" };
        var processName = EtlProcess.GetProcessName(etlName, transformationName);

        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            var taskId = AddEtlTask(src, dest, etlName, connectionStringName, [transformationName], [goodScript], collections);
            
            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadSuccesses == 10);

            var etlStats = await GetEtlStatsAsync(src, processName);
            Assert.Equal(0, etlStats.TransformationErrors);
            Assert.Equal(EtlProcessHealthStatus.Healthy, etlStats.HealthStatus);
            
            var brokenConfig = new RavenEtlConfiguration
            {
                Name = etlName,
                ConnectionStringName = connectionStringName,
                MentorNode = null,
                PinToMentorNode = false,
                Transforms =
                [
                    new Transformation
                    {
                        Name = transformationName,
                        Collections = collections,
                        Script = badScript,
                        ApplyToAllDocuments = false,
                        Disabled = false
                    }
                ]
            };
            var brokenUpdateResult = src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(taskId, brokenConfig));
            taskId = brokenUpdateResult.TaskId;

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, processName, stats => stats.TransformationErrors >= 10 && stats.HealthStatus == EtlProcessHealthStatus.Failed);

            etlStats = await GetEtlStatsAsync(src, processName);
            Assert.True(etlStats.TransformationErrors >= 10);
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
            
            var fixedConfig = new RavenEtlConfiguration
            {
                Name = etlName,
                ConnectionStringName = connectionStringName,
                MentorNode = null,
                PinToMentorNode = false,
                Transforms =
                [
                    new Transformation
                    {
                        Name = transformationName,
                        Collections = collections,
                        Script = goodScript,
                        ApplyToAllDocuments = false,
                        Disabled = false
                    }
                ]
            };
            var fixUpdateResult = src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(taskId, fixedConfig));
            taskId = fixUpdateResult.TaskId;

            using (var commands = src.Commands())
            {
                var deleteErrorsCommand = new DeleteEtlTaskErrorsCommand([processName]);
                await commands.ExecuteAsync(deleteErrorsCommand);
            }

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadSuccesses == 10);

            etlStats = await GetEtlStatsAsync(src, processName);
            Assert.Equal(0, etlStats.TransformationErrors);
            Assert.Equal(EtlProcessHealthStatus.Healthy, etlStats.HealthStatus);
            
            var disableResult = await dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true));
            Assert.True(disableResult.Disabled);

            src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(taskId, fixedConfig, [transformationName]));
            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadErrors > 0 && stats.HealthStatus == EtlProcessHealthStatus.Failed);

            etlStats = await GetEtlStatsAsync(src, processName);
            Assert.True(etlStats.LoadErrors > 0);
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestEwmaCalculation()
    {
        var ewma = new TimeAgnosticEwma();
        
        ewma.UpdateOnBatchCompletion(errorsInThisBatch: 10, totalItemsInThisBatch: 10);
        Assert.Equal(1.0, ewma.GetRate());
        
        ewma.UpdateOnBatchCompletion(errorsInThisBatch: 10, totalItemsInThisBatch: 60);
        Assert.InRange(ewma.GetRate(), 0.80, 0.81);
        
        ewma.UpdateOnBatchCompletion(errorsInThisBatch: 0, totalItemsInThisBatch: 900);
        Assert.InRange(ewma.GetRate(), 0.16, 0.17);
        
        ewma.UpdateOnBatchCompletion(errorsInThisBatch: 500, totalItemsInThisBatch: 510);
        Assert.InRange(ewma.GetRate(), 0.42, 0.43);
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task TestGetEtlErrorsEndpoint()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   this.Name = 'James Doe';
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            const string connectionStringName2 = "ConnectionString2";
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   this.Name = 'Cool Company';
                                   loadToCompanies(this);
                                   """;
            const string transformationName3 = "Transformation3";
            const string script3 = """
                                   this.Name = 'Other Company Name';
                                   loadToCompanies(this);
                                   """;
            var collections2 = new List<string>() { "Companies" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 5);

            AddEtlTask(src, dest, etlName2, connectionStringName2, [transformationName2, transformationName3], [script2, script3], collections2);

            var disableDatabaseResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();

            Assert.True(disableDatabaseResult.Disabled);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new Company() { Name = "Some Company" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName2), stats => stats.LoadErrors == 5);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName3), stats => stats.LoadErrors == 5);

            using (var commands = src.Commands())
            {
                var cmd = new GetEtlTaskErrorsCommand(new List<string>());
                await commands.ExecuteAsync(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);

                res.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<TaskErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName1));
                
                Assert.Empty(firstTaskErrors.ProcessErrors);
                Assert.Equal(5, firstTaskErrors.ItemErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName2));
                
                Assert.Contains(secondTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskErrors.ItemErrors);
                
                var thirdTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName3));

                Assert.Contains(thirdTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskErrors.ItemErrors);
                
                var cmdSingleTask = new GetEtlTaskErrorsCommand([EtlProcess.GetProcessName(etlName1, transformationName1)]);
                await commands.ExecuteAsync(cmdSingleTask);

                var resSingle = cmdSingleTask.Result as BlittableJsonReaderObject;
                resSingle.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray resultsSingle);
                var resultsSingleList = JsonConvert.DeserializeObject<List<TaskErrors>>(resultsSingle.ToString());

                Assert.Single(resultsSingleList);
                Assert.Equal(EtlProcess.GetProcessName(etlName1, transformationName1), resultsSingleList.Single().TaskName);
                Assert.Equal(5, resultsSingleList.Single().ItemErrors.Length);
                Assert.Empty(resultsSingleList.Single().ProcessErrors);
                
                var cmdTwoTasks = new GetEtlTaskErrorsCommand(
                [
                    EtlProcess.GetProcessName(etlName2, transformationName2),
                    EtlProcess.GetProcessName(etlName2, transformationName3)
                ]);
                await commands.ExecuteAsync(cmdTwoTasks);

                var resTwo = cmdTwoTasks.Result as BlittableJsonReaderObject;
                resTwo.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray resultsTwo);
                var resultsTwoList = JsonConvert.DeserializeObject<List<TaskErrors>>(resultsTwo.ToString());

                Assert.Equal(2, resultsTwoList.Count);
                Assert.DoesNotContain(resultsTwoList, x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName1));

                var secondTaskFiltered = resultsTwoList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName2));
                Assert.Contains(secondTaskFiltered.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskFiltered.ItemErrors);

                var thirdTaskFiltered = resultsTwoList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName3));
                Assert.Contains(thirdTaskFiltered.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskFiltered.ItemErrors);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task TestGetEtlErrorsEndpointForShardedDatabase(Options options)
    {
        const int shardNumber = 1;
        
        using (var src = GetDocumentStore(options))
        using (var dest = GetDocumentStore(options))
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   this.Name = 'James Doe';
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            const string connectionStringName2 = "ConnectionString2";
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   this.Name = 'Cool Company';
                                   loadToCompanies(this);
                                   """;
            const string transformationName3 = "Transformation3";
            const string script3 = """
                                   this.Name = 'Other Company Name';
                                   loadToCompanies(this);
                                   """;
            var collections2 = new List<string>() { "Companies" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new User { Id = $"Users/{i}${shardNumber}", Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 5, shardNumber);

            AddEtlTask(src, dest, etlName2, connectionStringName2, [transformationName2, transformationName3], [script2, script3], collections2);

            var disableDatabaseResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();

            Assert.True(disableDatabaseResult.Disabled);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new Company() { Id = $"Companies/{i}${shardNumber}", Name = "Some Company" });
                await session.SaveChangesAsync();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName2), stats => stats.LoadErrors == 5, shardNumber);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName3), stats => stats.LoadErrors == 5, shardNumber);

            using (var commands = src.Commands())
            {
                var cmd = new GetEtlTaskErrorsCommand(new List<string>(), isSharded: true, shardNumber);
                await commands.ExecuteAsync(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<TaskErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName1));
                
                Assert.Empty(firstTaskErrors.ProcessErrors);
                Assert.Equal(5, firstTaskErrors.ItemErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName2));
                
                Assert.Contains(secondTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskErrors.ItemErrors);
                
                var thirdTaskErrors = resultsObjectList.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName3));
                
                Assert.Contains(thirdTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskErrors.ItemErrors);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task TestDeleteEtlErrorsEndpoint()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   this.Name = 'James Doe';
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };

            const string connectionStringName2 = "ConnectionString2";
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   this.Name = 'Some Company';
                                   throw new Error("dummy error");
                                   loadToCompanies(this);
                                   """;
            var collections2 = new List<string>() { "Companies" };

            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            AddEtlTask(src, dest, etlName2, connectionStringName2, [transformationName2], [script2], collections2);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new Company { Name = "Some Company" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 5);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName2), stats => stats.TransformationErrors == 5);

            using (var commands = src.Commands())
            {
                await commands.ExecuteAsync(new DeleteEtlTaskErrorsCommand([EtlProcess.GetProcessName(etlName1, transformationName1)]));

                var getAfterFirstDelete = new GetEtlTaskErrorsCommand(
                [
                    EtlProcess.GetProcessName(etlName1, transformationName1),
                    EtlProcess.GetProcessName(etlName2, transformationName2)
                ]);
                await commands.ExecuteAsync(getAfterFirstDelete);

                var res1 = getAfterFirstDelete.Result as BlittableJsonReaderObject;
                Assert.NotNull(res1);
                res1.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray results1);
                var list1 = JsonConvert.DeserializeObject<List<TaskErrors>>(results1.ToString());

                var etl1AfterDelete = list1.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName1));
                Assert.Empty(etl1AfterDelete.ProcessErrors);
                Assert.Empty(etl1AfterDelete.ItemErrors);

                var etl2Untouched = list1.Single(x => x.TaskName == EtlProcess.GetProcessName(etlName2, transformationName2));
                Assert.Equal(5, etl2Untouched.ItemErrors.Length);
                
                await commands.ExecuteAsync(new DeleteEtlTaskErrorsCommand(
                [
                    EtlProcess.GetProcessName(etlName1, transformationName1),
                    EtlProcess.GetProcessName(etlName2, transformationName2)
                ]));

                var getAfterSecondDelete = new GetEtlTaskErrorsCommand(
                [
                    EtlProcess.GetProcessName(etlName1, transformationName1),
                    EtlProcess.GetProcessName(etlName2, transformationName2)
                ]);
                await commands.ExecuteAsync(getAfterSecondDelete);

                var res2 = getAfterSecondDelete.Result as BlittableJsonReaderObject;
                Assert.NotNull(res2);
                res2.TryGet(nameof(TaskErrorsResponse.Results), out BlittableJsonReaderArray results2);
                var list2 = JsonConvert.DeserializeObject<List<TaskErrors>>(results2.ToString());

                Assert.Equal(2, list2.Count);
                Assert.All(list2, t =>
                {
                    Assert.Empty(t.ProcessErrors);
                    Assert.Empty(t.ItemErrors);
                });
            }
        }
    }
        
    [RavenFact(RavenTestCategory.Etl)]
    public async Task TestEtlHealthStatusUpdates()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe")
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 10 && stats.HealthStatus == EtlProcessHealthStatus.Failed);

            var etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(0, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 50; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe", Value = 1 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 50);

            etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(50, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);
            Assert.Equal(EtlProcessHealthStatus.Impaired, etlStats.HealthStatus);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 900; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe", Value = 1 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 950);
            
            etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));
            
            Assert.Equal(950, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);
            Assert.Equal(EtlProcessHealthStatus.Impaired, etlStats.HealthStatus);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 1000; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe", Value = 1 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 960);

            etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(960, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);

            Assert.Equal(EtlProcessHealthStatus.Impaired, etlStats.HealthStatus);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task HealthStatusUpdatesShouldRespectConfiguration()
    {
        var options = new Options()
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold)] = "0.01";
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusImpairedThreshold)] = "0.001";
            }
        };
        
        using (var src = GetDocumentStore(options))
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe")
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
                    
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 9; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe", Value = 0 });

                await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.HealthStatus == EtlProcessHealthStatus.Failed);
                    
            var etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);

            await src.Maintenance.SendAsync(new PutDatabaseSettingsOperation(src.Database, new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold)] = "0.1",
                [RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusImpairedThreshold)] = "0.9"
            }));

            var ex = Assert.Throws<InvalidOperationException>(() =>
                Server.ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(src.Database));
            Assert.Contains(RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold), ex.Message);
            Assert.Contains(RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusImpairedThreshold), ex.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task InvalidScriptShouldSetTaskHealthToFailed()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string processTag = "Raven ETL";
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   var x = ;

                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.HealthStatus == EtlProcessHealthStatus.Failed);

            var etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
            await AssertHealthStatusNotificationAsync(src, processTag, EtlProcess.GetProcessName(etlName1, transformationName1), EtlProcessHealthStatus.Failed);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task EtlProcessHealthStatusChangeNotificationsShouldBeAddedAndRemoved()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string processTag = "Raven ETL";
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe")
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 10);

            var etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(0, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);

            await AssertHealthStatusNotificationAsync(src, processTag, EtlProcess.GetProcessName(etlName1, transformationName1), EtlProcessHealthStatus.Failed);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 50; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe", Value = 1 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 50);

            etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(50, etlStats.LoadSuccesses);
            Assert.NotEqual(0, etlStats.TransformationErrors);

            await AssertHealthStatusNotificationAsync(src, processTag, EtlProcess.GetProcessName(etlName1, transformationName1), EtlProcessHealthStatus.Impaired);

            const int healthyBatches = 10;
            const int healthyBatchSize = 200;

            for (int batch = 0; batch < healthyBatches; batch++)
            {
                using (var session = src.OpenAsyncSession())
                {
                    for (int i = 0; i < healthyBatchSize; i++)
                        await session.StoreAsync(new User { Name = "Joe Doe", Value = 1 });
                    await session.SaveChangesAsync();
                }
            }

            const int expectedLoadSuccesses = 50 + healthyBatches * healthyBatchSize;
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1),
                stats => stats.LoadSuccesses == expectedLoadSuccesses && stats.HealthStatus == EtlProcessHealthStatus.Healthy);

            etlStats = await GetEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1));

            Assert.Equal(expectedLoadSuccesses, etlStats.LoadSuccesses);
            Assert.Equal(EtlProcessHealthStatus.Healthy, etlStats.HealthStatus);

            await AssertHealthStatusNotificationAsync(src, processTag, EtlProcess.GetProcessName(etlName1, transformationName1), EtlProcessHealthStatus.Healthy);
        }
    }

    private async Task AssertHealthStatusNotificationAsync(IDocumentStore store, string processTag, string processName, EtlProcessHealthStatus healthStatus)
    {
        var db = await GetDatabase(store.Database);

        var expectedMessage = healthStatus == EtlProcessHealthStatus.Healthy
            ? $"Task recovered to {nameof(EtlProcessHealthStatus.Healthy)} status."
            : $"Task health status was changed to {healthStatus}.";

        await WaitForAssertionAsync(() =>
        {
            var alert = db.NotificationCenter.EtlNotifications.GetAlert<EtlTaskHealthChangeDetails>(processTag, processName, AlertReason.Etl_HealthStatusChange);
            Assert.NotNull(alert);
            Assert.Equal(expectedMessage, alert.Message);
            return Task.CompletedTask;
        });
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task AssertTableRecordsAreDeletedOnConfigurationUpdate()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };

            var taskId1 = AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 10);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName2), stats => stats.TransformationErrors == 10);
            
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            
            Assert.Equal(20, WaitForValue(() => database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl).Count, 20, interval: 500));
            
            var updatedConfig = new RavenEtlConfiguration
            {
                Name = etlName1,
                ConnectionStringName = connectionStringName1,
                MentorNode = null,
                Transforms = [
                    new Transformation()
                    {
                        Name = transformationName1,
                        Collections = collections1,
                        Script = script1,
                        ApplyToAllDocuments = false,
                        Disabled = false
                    }
                ],
                PinToMentorNode = false
            };

            UpdateRavenEtlTask(src, taskId1, updatedConfig);
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 10);
            
            Assert.Equal(10, WaitForValue(() => database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl).Count, 10, interval: 500));

            var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);

            foreach (var itemError in itemErrors)
            {
                Assert.Equal(EtlProcess.GetProcessName(etlName1, transformationName1), itemError.TaskName);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public async Task AssertTableRecordsAreDeletedOnTaskDeletion()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            var collections1 = new List<string>() { "Users" };
            
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var taskId1 = AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            _ = AddEtlTask(src, dest, etlName2, connectionStringName1, [transformationName2], [script2], collections1);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "James Doe", Value = 0 });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 10);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName2, transformationName2), stats => stats.TransformationErrors == 10);
            
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            
            Assert.Equal(20, WaitForValue(() => database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl).Count, 20, interval: 500));
            
            var deleteOp = new DeleteOngoingTaskOperation(taskId1, OngoingTaskType.RavenEtl);
            src.Maintenance.Send(deleteOp);
            
            Assert.Equal(10, WaitForValue(() => database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl).Count, 10, interval: 500));

            var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);

            foreach (var itemError in itemErrors)
            {
                Assert.Equal(EtlProcess.GetProcessName(etlName2, transformationName2), itemError.TaskName);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task TaskErrorsArePreservedOnEtlReset()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName = "ConnectionString1";
            const string etlName = "ETL1";
            const string transformationName = "Transformation1";
            const string script = "loadToUsers(this);";
            var collections = new List<string> { "Users" };
            var processName = EtlProcess.GetProcessName(etlName, transformationName);
            const int errorsCount = 5;

            AddEtlTask(src, dest, etlName, connectionStringName, [transformationName], [script], collections);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < errorsCount; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }

            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadSuccesses == errorsCount);

            var database = await GetDatabase(src.Database);

            for (int i = 0; i < errorsCount; i++)
            {
                database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
                {
                    CreatedAt = DateTime.UtcNow,
                    TaskName = processName,
                    AffectedDocumentsCount = 1,
                    Step = TaskErrorStep.Load,
                    Error = $"injected error {i}"
                });
            }

            Assert.Equal(errorsCount, database.TaskErrorsStorage.ReadAllProcessErrors(TaskCategory.Etl).Count);
            
            var resetDone = Etl.WaitForEtlToComplete(src, (_, _) => true);

            await src.Maintenance.SendAsync(new ResetEtlOperation(etlName, transformationName));

            Assert.True(await resetDone.WaitAsync(TimeSpan.FromSeconds(30)));
            
            Assert.Equal(errorsCount, database.TaskErrorsStorage.ReadAllProcessErrors(TaskCategory.Etl).Count);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task ErrorsLimitInStorageShouldBeRespected()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe")
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   if (this.Value == 1)
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            
            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);
            
            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 650; i++)
                    session.Store(new User { Name = "James Doe", Value = 0 });
                
                for (int i = 0; i < 50; i++)
                    session.Store(new User { Name = "James Doe", Value = 1 });
                
                session.SaveChanges();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors >= 700);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName2), stats => stats.TransformationErrors >= 50);
            
            var database = GetDatabase(src.Database).Result;
            
            var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);

            var firstTransformationErrors = itemErrors.Where(x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName1));
            var secondTransformationErrors = itemErrors.Where(x => x.TaskName == EtlProcess.GetProcessName(etlName1, transformationName2));

            Assert.Equal(500, firstTransformationErrors.Count());
            Assert.Equal(50, secondTransformationErrors.Count());
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestScriptErrorsShouldNotBePersisted()
    {
        using (var store = GetDocumentStore())
        {
            var user = new User() { Id = "users/1", Name = "Joe Doe" };
            
            using (var session = store.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testResult = RavenEtl.TestScript(new TestRavenEtlScript
                {
                    DocumentId = user.Id,
                    Configuration = new RavenEtlConfiguration()
                    {
                        Name = "simulate",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = { "Users" },
                                Name = "Users",
                                Script =
                                    """
                                    throw new Error("dummy error"); 
                                    loadToUsers(this);
                                    """
                            }
                        }
                    }
                }, database, database.ServerStore, context);
                
                var result = (RavenEtlTestScriptResult)testResult;
                Assert.Single(result.TransformationErrors);
                
                var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);
                Assert.Empty(itemErrors);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Etl)]
    public async Task CanGetEtlErrorsSnmpMetrics_V2C()
    {
        var port = ReservePort().Port;
        var communityString = "public-test";
        var customSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)] = "V2C",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = port.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Community)] = communityString
        };

        UseNewLocalServer(customSettings);
        
        using (var src = GetDocumentStore(new Options { CreateDatabase = true }))
        using (var dest = GetDocumentStore(new Options { CreateDatabase = true }))
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";

            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe") {
                                       throw new Error("dummy error");
                                   }
                                   
                                   loadToUsers(this);
                                   """;

            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);

            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 123; i++)
                    session.Store(new User { Name = "James Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.TransformationErrors == 123);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName2), stats => stats.TransformationErrors == 123);

            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 4; i++)
                    session.Store(new User { Name = "Joe Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 4);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName2), stats => stats.TransformationErrors == 127);
            
            var ip = new Uri(Server.WebUrl).Host;
            var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            
            using (var commands = src.Commands())
            {
                var cmd = new GetSnmpOidsCommand();

                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet("Databases", out BlittableJsonReaderObject databases);
                res.TryGet("Server", out BlittableJsonReaderArray server);
                
                var serverOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(server.ToString());

                var serverEtlErrorsOid = serverOidsObjectList.Single(x => x.Description == "Number of ETL errors").OID;
                
                var result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverEtlErrorsOid))],
                    10000);
                
                Assert.Equal(250, ((Integer32)result.Single().Data).ToInt32());
                
                var serverHealthyEtlsCount = serverOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Healthy)} ETL tasks count").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverHealthyEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var serverImpairedEtlsCount = serverOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Impaired)} ETL tasks count").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverImpairedEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var serverFailedEtlsCount = serverOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Failed)} ETL tasks count").OID;
                                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverFailedEtlsCount))],
                    10000);
                                
                Assert.Equal(2, ((Integer32)result.Single().Data).ToInt32());
                
                databases.TryGet(src.Database, out BlittableJsonReaderObject databaseOids);
                
                databaseOids.TryGet("@General", out BlittableJsonReaderArray generalEntries);
                databaseOids.TryGet("Etls", out BlittableJsonReaderObject etlEntries);
                
                var databaseOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(generalEntries.ToString());

                var databaseEtlErrorsOid = databaseOidsObjectList.Single(x => x.Description == "Number of ETL errors").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseEtlErrorsOid))],
                    10000);
                
                Assert.Equal(250, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseHealthyEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Healthy)} ETL tasks count").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseHealthyEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseImpairedEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Impaired)} ETL tasks count").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseImpairedEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseFailedEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"{nameof(EtlProcessHealthStatus.Failed)} ETL tasks count").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseFailedEtlsCount))],
                    10000);
                
                Assert.Equal(2, ((Integer32)result.Single().Data).ToInt32());
                
                etlEntries.TryGet(EtlProcess.GetProcessName(etlName1, transformationName1), out BlittableJsonReaderArray firstProcessEntries);
                var firstProcessOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(firstProcessEntries.ToString());
                var firstProcessEtlErrorsOid = firstProcessOidsObjectList.Single(x => x.Description == "Number of task ETL errors").OID;
                var firstProcessHealthStatusOid = firstProcessOidsObjectList.Single(x => x.Description == "Health status of particular ETL task").OID;
                var firstProcessLastSuccessfulBatchTimeOid = firstProcessOidsObjectList.Single(x => x.Description == "Last successful batch time").OID;
                var firstProcessResponsibleNodeOid = firstProcessOidsObjectList.Single(x => x.Description == "Responsible node tag of particular ETL task").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessEtlErrorsOid))],
                    10000);
                
                Assert.Equal(123, ((Integer32)result.Single().Data).ToInt32());
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessHealthStatusOid))],
                    10000);
                
                Assert.Equal("Failed", result.Single().Data.ToString());
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessLastSuccessfulBatchTimeOid))],
                    10000);
                
                Assert.Equal(SnmpType.TimeTicks, result.Single().Data.TypeCode);

                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessResponsibleNodeOid))],
                    10000);

                Assert.Equal(Server.ServerStore.NodeTag, result.Single().Data.ToString());

                etlEntries.TryGet(EtlProcess.GetProcessName(etlName1, transformationName2), out BlittableJsonReaderArray secondProcessEntries);
                var secondProcessOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(secondProcessEntries.ToString());
                var secondProcessEtlErrorsOid = secondProcessOidsObjectList.Single(x => x.Description == "Number of task ETL errors").OID;
                var secondProcessHealthStatusOid = secondProcessOidsObjectList.Single(x => x.Description == "Health status of particular ETL task").OID;
                var secondProcessLastSuccessfulBatchTimeOid = secondProcessOidsObjectList.Single(x => x.Description == "Last successful batch time").OID;
                var secondProcessResponsibleNodeOid = secondProcessOidsObjectList.Single(x => x.Description == "Responsible node tag of particular ETL task").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(secondProcessEtlErrorsOid))],
                    10000);
                
               Assert.Equal(127, ((Integer32)result.Single().Data).ToInt32());
               
               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(secondProcessHealthStatusOid))],
                   10000);
               
               Assert.Equal("Failed", result.Single().Data.ToString());
               
               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(secondProcessLastSuccessfulBatchTimeOid))],
                   10000);

               Assert.Equal(SnmpType.TimeTicks, result.Single().Data.TypeCode);
               Assert.Equal(0u, ((TimeTicks)result.Single().Data).ToUInt32());

               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(secondProcessResponsibleNodeOid))],
                   10000);

               Assert.Equal(Server.ServerStore.NodeTag, result.Single().Data.ToString());

               databases.TryGet("@General", out BlittableJsonReaderArray databasesGeneralEntries);
               var databasesGeneralOidsList = JsonConvert.DeserializeObject<List<SnmpEntry>>(databasesGeneralEntries.ToString());

               var totalEtlDocumentsProcessedPerSecOid = databasesGeneralOidsList
                   .Single(x => x.Description == "Number of documents processed per second by all ETL tasks across all databases (one minute rate)").OID;

               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(totalEtlDocumentsProcessedPerSecOid))],
                   10000);

               Assert.Equal(SnmpType.Gauge32, result.Single().Data.TypeCode);

               var totalAiTaskDocumentsProcessedPerSecOid = databasesGeneralOidsList
                   .Single(x => x.Description == "Number of documents processed per second by all AI tasks across all databases (one minute rate)").OID;

               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(totalAiTaskDocumentsProcessedPerSecOid))],
                   10000);

               Assert.Equal(SnmpType.Gauge32, result.Single().Data.TypeCode);
            }
        }
    }

    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Etl)]
    public async Task EtlsMonitoringEndpointShouldWork()
    {
        using (var src = GetDocumentStore(new Options { CreateDatabase = true }))
        using (var dest = GetDocumentStore(new Options { CreateDatabase = true }))
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";

            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe") {
                                       throw new Error("dummy error");
                                   }

                                   loadToUsers(this);
                                   """;

            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);

            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 123; i++)
                    session.Store(new User { Name = "Joe Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName1), stats => stats.LoadSuccesses == 123);
            await WaitForEtlStatsAsync(src, EtlProcess.GetProcessName(etlName1, transformationName2), stats => stats.TransformationErrors == 123);
            
            using (var commands = src.Commands())
            {
                var cmd = new GetEtlsMonitoringDataCommand();

                await commands.ExecuteAsync(cmd);

                var resBjro = cmd.Result as BlittableJsonReaderObject;
                var results = JsonConvert.DeserializeObject<EtlsMetrics>(resBjro.ToString()).Results;
                var databaseResults = results.Single(x => x.DatabaseName == src.Database);
                
                var firstProcessResults = databaseResults.Etls.Single(x => x.ProcessName == EtlProcess.GetProcessName(etlName1, transformationName1));
                Assert.Equal(EtlProcessHealthStatus.Healthy, firstProcessResults.HealthStatus);
                Assert.NotNull(firstProcessResults.LastSuccessfulBatchTimeInSec);
                Assert.Equal(0, firstProcessResults.ErrorsCount);
                
                var secondProcessResults = databaseResults.Etls.Single(x => x.ProcessName == EtlProcess.GetProcessName(etlName1, transformationName2));
                Assert.Equal(EtlProcessHealthStatus.Failed, secondProcessResults.HealthStatus);
                Assert.Null(secondProcessResults.LastSuccessfulBatchTimeInSec);
                Assert.Equal(123, secondProcessResults.ErrorsCount);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task ForceBatchRetryShouldWakeUpEtlProcessFromFallbackMode()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName = "ConnectionString1";
            const string etlName = "ETL1";
            const string transformationName = "Transformation1";
            const string script = "loadToUsers(this);";
            var collections = new List<string> { "Users" };

            var database = await GetDatabase(src.Database);
            
            database.ForTestingPurposes ??= new DocumentDatabase.TestingStuff();
            database.ForTestingPurposes.EtlFallbackTime = TimeSpan.FromMinutes(60);

            AddEtlTask(src, dest, etlName, connectionStringName, [transformationName], [script], collections);

            var processName = EtlProcess.GetProcessName(etlName, transformationName);
            
            var disableResult = await dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true));
            Assert.True(disableResult.Disabled);

            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                    await session.StoreAsync(new User { Name = "Joe Doe" });
                await session.SaveChangesAsync();
            }
            
            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadErrors > 0);

            var process = database.EtlLoader.Processes.Single(x => x.Name == processName);

            Assert.Equal(OngoingTaskConnectionStatus.Reconnect, process.GetConnectionStatus());
            
            await dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: false));
            
            process.ForceBatchRetry();
            
            await WaitForEtlStatsAsync(src, processName, stats => stats.LoadSuccesses >= 5, timeout: 30_000);

            var etlStats = await GetEtlStatsAsync(src, processName);
            Assert.True(etlStats.LoadSuccesses >= 5);
            Assert.Equal(OngoingTaskConnectionStatus.Active, process.GetConnectionStatus());
        }
    }

    private async Task<EtlProcessStatistics> GetEtlStatsAsync(DocumentStore store, string etlName, int shardNumber = 0)
    {
        var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

        DocumentDatabase database;
        if (record.IsSharded)
        {
            var bucket = await Sharding.GetBucketAsync(store, shardNumber.ToString());
            database = await Sharding.GetShardedDocumentDatabaseForBucketAsync(store.Database, bucket);
        }
        else
        {
            database = await GetDatabase(store.Database);
        }
        
        var etl = database.EtlLoader.Processes.Single(x => x.Name == etlName);

        return etl.Statistics;
    }

    private async Task WaitForEtlStatsAsync(DocumentStore store, string etlName, Func<EtlProcessStatistics, bool> predicate = null, int shardNumber = 0, int timeout = 10_000, int interval = 500)
    {
        predicate ??= _ => true;
        await WaitForPredicateAsync(x => predicate(x), async () => await GetEtlStatsAsync(store, etlName, shardNumber), timeout, interval);
    }

    private static long AddEtlTask(DocumentStore src, DocumentStore dest, string etlName, string connectionStringName, List<string> transformationNames, List<string> transformationScripts, List<string> collections)
    {
        var configuration = new RavenEtlConfiguration
        {
            Name = etlName,
            ConnectionStringName = connectionStringName,
            MentorNode = null,
            Transforms = [],
            PinToMentorNode = false
        };

        foreach ((string transformationName, string transformationScript) in transformationNames.Zip(transformationScripts))
        {
            var transformation = new Transformation
            {
                Name = transformationName,
                Collections = collections,
                Script = transformationScript,
                ApplyToAllDocuments = false,
                Disabled = false
            };
            
            configuration.Transforms.Add(transformation);
        }

        var connectionString = new RavenConnectionString
        {
            Name = connectionStringName,
            Database = dest.Database,
            TopologyDiscoveryUrls = dest.Urls
        };

        src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
        var result = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
        
        return result.TaskId;
    }

    private static void UpdateRavenEtlTask(IDocumentStore store, long taskId, RavenEtlConfiguration configuration)
    {
        store.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(taskId, configuration));
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class SnmpEntry
    {
        public string OID { get; set; }
        public string Description { get; set; }
    }
    
    private class GetEtlTaskErrorsCommand : RavenCommand<object>
    {
        private readonly List<string> _taskNames;
        private readonly bool _isSharded;
        private readonly int _shardNumber;
        
        public GetEtlTaskErrorsCommand(List<string> taskNames, bool isSharded = false, int shardNumber = 0)
        {
            _taskNames = taskNames;
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var baseUrl = $"{node.Url}/databases/{node.Database}/etl/errors";

            var queryParams = new List<KeyValuePair<string, StringValues>>
            {
                new("name", new StringValues(_taskNames.ToArray()))
            };

            if (_isSharded)
            {
                queryParams.Add(new KeyValuePair<string, StringValues>("nodeTag", node.ClusterTag));
                queryParams.Add(new KeyValuePair<string, StringValues>("shardNumber", _shardNumber.ToString()));
            }

            url = QueryHelpers.AddQueryString(baseUrl, queryParams);

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
    }
    
    private class DeleteEtlTaskErrorsCommand : RavenCommand<object>
    {
        private readonly List<string> _taskNames;
        private readonly bool _isSharded;
        private readonly int _shardNumber;

        public DeleteEtlTaskErrorsCommand(List<string> taskNames, bool isSharded = false, int shardNumber = 0)
        {
            _taskNames = taskNames;
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var baseUrl = $"{node.Url}/databases/{node.Database}/etl/errors";

            var queryParams = new List<KeyValuePair<string, StringValues>>
            {
                new("name", new StringValues(_taskNames.ToArray()))
            };

            if (_isSharded)
            {
                queryParams.Add(new KeyValuePair<string, StringValues>("nodeTag", node.ClusterTag));
                queryParams.Add(new KeyValuePair<string, StringValues>("shardNumber", _shardNumber.ToString()));
            }

            url = QueryHelpers.AddQueryString(baseUrl, queryParams);

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
    }
    
    private class GetSnmpOidsCommand : RavenCommand<object>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/monitoring/snmp/oids";
            
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
    
            Result = response;
        }
    
        public override bool IsReadRequest => true;
    }

    private class GetEtlsMonitoringDataCommand : RavenCommand<object>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/monitoring/v1/etls";
            
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
    
            Result = response;
        }
    
        public override bool IsReadRequest => true;
    }
}

public class RavenDB_21192_Multinode : ClusterTestBase
{
    public RavenDB_21192_Multinode(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Cluster)]
    public async Task EtlErrorsShouldBeStoredOnResponsibleNodeInCluster()
    {
        const string connectionStringName = "ConnectionString1";
        const string etlName = "ETL1";
        const string transformationName = "Transformation1";
        const string script = """
                              if (this.Name == "James Doe")
                              {
                                   throw new Error("dummy error");
                              }
                              loadToUsers(this);
                              """;
        var collections = new List<string> { "Users" };

        const int clusterSize = 3;

        var (nodes, leader) = await CreateRaftCluster(clusterSize);

        var srcDatabaseName = GetDatabaseName();
        var dstDatabaseName = GetDatabaseName();

        await CreateDatabaseInCluster(srcDatabaseName, 3, leader.WebUrl);
        await CreateDatabaseInCluster(dstDatabaseName, 1, leader.WebUrl);

        using var src = new DocumentStore
        {
            Urls = nodes.Select(n => n.WebUrl).ToArray(),
            Database = srcDatabaseName
        }.Initialize();

        using var dest = new DocumentStore
        {
            Urls = new[] { leader.WebUrl },
            Database = dstDatabaseName
        }.Initialize();
        
        var mentorTag = nodes[0].ServerStore.NodeTag;

        var configuration = new RavenEtlConfiguration
        {
            Name = etlName,
            ConnectionStringName = connectionStringName,
            MentorNode = mentorTag,
            PinToMentorNode = true,
            Transforms =
            {
                new Transformation
                {
                    Name = transformationName,
                    Collections = collections,
                    Script = script,
                    ApplyToAllDocuments = false,
                    Disabled = false
                }
            }
        };

        var connectionString = new RavenConnectionString
        {
            Name = connectionStringName,
            Database = dest.Database,
            TopologyDiscoveryUrls = dest.Urls.ToArray()
        };

        src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
        var addResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
        Assert.NotNull(addResult.RaftCommandIndex);
        
        using (var session = src.OpenSession())
        {
            for (int i = 0; i < 5; i++)
                session.Store(new User { Name = "James Doe" });

            session.SaveChanges();
        }
        
        using (var session = src.OpenSession())
        {
            for (int i = 0; i < 3; i++)
                session.Store(new User { Name = "Joe Doe" });

            session.SaveChanges();
        }
        
        var mentorNode = nodes[0];
        var mentorDatabase = await GetDatabase(mentorNode, srcDatabaseName);
        
        Assert.True(WaitForValue(() =>
        {
            var process = mentorDatabase.EtlLoader.Processes.FirstOrDefault(x => x.Name == $"{etlName}/{transformationName}");
            return process?.Statistics.TransformationErrors >= 5;
        }, true, timeout: 30_000));

        Assert.True(WaitForValue(() =>
        {
            var process = mentorDatabase.EtlLoader.Processes.FirstOrDefault(x => x.Name == $"{etlName}/{transformationName}");
            return process?.Statistics.LoadSuccesses >= 3;
        }, true, timeout: 30_000));

        Assert.True(WaitForValue(() => mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}").Count == 5, true, timeout: 30_000));
        var mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
        Assert.Equal(5, mentorItemErrors.Count);
        
        for (int i = 1; i < clusterSize; i++)
        {
            var otherDatabase = await GetDatabase(nodes[i], srcDatabaseName);
            var otherItemErrors = otherDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
            Assert.Empty(otherItemErrors);
        }
        
        var newMentorTag = nodes[1].ServerStore.NodeTag;
        configuration.MentorNode = newMentorTag;
        src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(addResult.TaskId, configuration));

        var newMentorNode = nodes[1];
        var newMentorDatabase = await GetDatabase(newMentorNode, srcDatabaseName);
        
        Assert.True(WaitForValue(() =>
        {
            return newMentorDatabase.EtlLoader.Processes.Any(x => x.Name == $"{etlName}/{transformationName}");
        }, true, timeout: 30_000));

        using (var session = src.OpenSession())
        {
            for (int i = 0; i < 7; i++)
                session.Store(new User { Name = "James Doe" });

            session.SaveChanges();
        }
        
        Assert.True(WaitForValue(() =>
        {
            var process = newMentorDatabase.EtlLoader.Processes.FirstOrDefault(x => x.Name == $"{etlName}/{transformationName}");
            return process?.Statistics.TransformationErrors >= 7;
        }, true, timeout: 30_000));

        Assert.True(WaitForValue(() => newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}").Count == 7, true, timeout: 30_000));
        var newMentorItemErrors = newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
        Assert.Equal(7, newMentorItemErrors.Count);

        mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
        Assert.Equal(5, mentorItemErrors.Count);
        
        newMentorDatabase.TaskErrorsStorage.DeleteErrorsOfTask($"{etlName}/{transformationName}", TaskCategory.Etl);
        
        newMentorItemErrors = newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
        Assert.Empty(newMentorItemErrors);
        
        mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl,$"{etlName}/{transformationName}");
        Assert.Equal(5, mentorItemErrors.Count);
    }
    
    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Cluster)]
    public async Task EmbeddingsGenerationErrorsShouldBeStoredOnResponsibleNodeInCluster()
    {
        const string connectionStringName = "AI-ConnectionString";
        const string taskName = "EmbeddingsTask1";
        const string processName = $"{taskName}/embeddings-transform-script";
        const string script = """
                              if (this.Name === "Error Document")
                              {
                                   throw new Error("dummy error");
                              }
                              embeddings.generate({ Name: this.Name });
                              """;

        const int clusterSize = 3;

        var (nodes, leader) = await CreateRaftCluster(clusterSize);

        var databaseName = GetDatabaseName();
        await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);

        using var store = new DocumentStore
        {
            Urls = nodes.Select(n => n.WebUrl).ToArray(),
            Database = databaseName
        }.Initialize();

        var mentorTag = nodes[0].ServerStore.NodeTag;

        var connectionString = new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.TextEmbeddings,
            EmbeddedSettings = new EmbeddedSettings()
        };
        connectionString.Identifier = connectionString.GenerateIdentifier();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = taskName,
            ConnectionStringName = connectionStringName,
            MentorNode = mentorTag,
            PinToMentorNode = true,
            Collection = "Users",
            EmbeddingsTransformation = new EmbeddingsTransformation
            {
                Script = script,
                ChunkingOptions = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
            },
            ChunkingOptionsForQuerying = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
        };
        configuration.Identifier = configuration.GenerateIdentifier();

        var addResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));
        Assert.NotNull(addResult.RaftCommandIndex);

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 5; i++)
                session.Store(new User { Name = "Error Document" });

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 3; i++)
                session.Store(new User { Name = "Joe Doe" });

            session.SaveChanges();
        }

        var mentorNode = nodes[0];
        var mentorDatabase = await GetDatabase(mentorNode, databaseName);

        Assert.True(WaitForValue(() =>
        {
            var process = mentorDatabase.EtlLoader.Processes.FirstOrDefault(x => x.Name == processName);
            return process?.Statistics.TransformationErrors >= 5 && process?.Statistics.LoadSuccesses >= 3;
        }, true, timeout: 30_000));

        Assert.True(WaitForValue(() => mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName).Count == 5, true, timeout: 30_000));
        var mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
        Assert.Equal(5, mentorItemErrors.Count);

        for (int i = 1; i < clusterSize; i++)
        {
            var otherDatabase = await GetDatabase(nodes[i], databaseName);
            var otherItemErrors = otherDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
            Assert.Empty(otherItemErrors);
        }

        var newMentorTag = nodes[1].ServerStore.NodeTag;
        configuration.MentorNode = newMentorTag;
        store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(addResult.TaskId, configuration));

        var newMentorNode = nodes[1];
        var newMentorDatabase = await GetDatabase(newMentorNode, databaseName);

        Assert.True(WaitForValue(() =>
        {
            return newMentorDatabase.EtlLoader.Processes.Any(x => x.Name == processName);
        }, true, timeout: 30_000));

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 7; i++)
                session.Store(new User { Name = "Error Document" });

            session.SaveChanges();
        }

        Assert.True(WaitForValue(() =>
        {
            var process = newMentorDatabase.EtlLoader.Processes.FirstOrDefault(x => x.Name == processName);
            return process?.Statistics.TransformationErrors >= 7;
        }, true, timeout: 30_000));

        Assert.True(WaitForValue(() => newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName).Count == 7, true, timeout: 30_000));
        var newMentorItemErrors = newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
        Assert.Equal(7, newMentorItemErrors.Count);

        mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
        Assert.Equal(5, mentorItemErrors.Count);

        newMentorDatabase.TaskErrorsStorage.DeleteErrorsOfTask(processName, TaskCategory.Ai);

        newMentorItemErrors = newMentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
        Assert.Empty(newMentorItemErrors);

        mentorItemErrors = mentorDatabase.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Ai,processName);
        Assert.Equal(5, mentorItemErrors.Count);
    }

    private class User
    {
        public string Name { get; set; }
    }
}
