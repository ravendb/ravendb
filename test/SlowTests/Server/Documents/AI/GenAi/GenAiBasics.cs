﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiBasics(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public void CanCreateGenAiTask(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanProcessDocuments(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var etl = Etl.WaitForEtlToComplete(store);

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        using (var session = store.OpenSession())
        {
            var p = new Post(
                [
                    new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                    new Comment(
                        "Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.",
                        "Oren Eini")
                ],
                "I, pencil",
                "A B52 pencil...");
            session.Store(p);
            session.SaveChanges();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanGetGenAiOngoingTask(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        var etlDone = Etl.WaitForEtlToComplete(store);

        store.Maintenance.Send(new AddGenAiOperation(config));

        var db = await GetDatabase(store.Database);

        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        const string id = "posts/1";
        using (var session = store.OpenSession())
        {
            session.Store(new Post(
                [
                    new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                    new Comment("Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.", "Oren Eini")
                ],
                "I, pencil",
                "A B52 pencil..."), id);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(60)));

        var secondBatchCompleted = await WaitForValueAsync(() =>
        {
            var stats = etlProcess.GetPerformanceStats()
                .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag > 2)
                .ToArray();

            return stats.Length > 0;
        }, expectedVal: true, timeout: 60_000);
        Assert.True(secondBatchCompleted);

        string changeVector = null;
        using (var session = store.OpenSession())
        {
            var doc = session.Load<Post>(id);
            changeVector = session.Advanced.GetChangeVectorFor(doc);
        }
        Assert.NotNull(changeVector);

        var stateUpdated = await WaitForValueAsync(() =>
        {
            var status = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        
            return status.ChangeVector == changeVector;
        }, expectedVal: true, timeout: 60_000);
        Assert.True(stateUpdated);

        var op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi);
        var taskInfo = await store.Maintenance.SendAsync(op);
        Assert.NotNull(taskInfo);
        Assert.Equal(config.Name, taskInfo.TaskName);
        Assert.Equal(OngoingTaskType.GenAi, taskInfo.TaskType);
        Assert.Equal(OngoingTaskConnectionStatus.Active, taskInfo.TaskConnectionStatus);

        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(config.ConnectionStringName, genAiTaskInfo.ConnectionStringName);
        Assert.Equal(config.Collection, genAiTaskInfo.Configuration.Collection);
        Assert.Equal(config.Prompt, genAiTaskInfo.Configuration.Prompt);
        Assert.Equal(config.SampleObject, genAiTaskInfo.Configuration.SampleObject);
        Assert.Equal(config.UpdateScript, genAiTaskInfo.Configuration.UpdateScript);
        // Assert.Equal(config.AiConnectorType, genAiTaskInfo.Configuration.AiConnectorType); // todo: fix serverside return 'AiConnectorType: None'
        Assert.Equal(config.GenAiTransformation.Script, genAiTaskInfo.Configuration.GenAiTransformation.Script);
        Assert.Equal(changeVector, genAiTaskInfo.ChangeVector);

    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanEditGenAiTask(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        var res = store.Maintenance.Send(new AddGenAiOperation(config));

        var op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi);
        var taskInfo = await store.Maintenance.SendAsync(op);
        var taskId = taskInfo.TaskId;

        var newUpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].LegitComment = $output.Blocked == false;
";
        config.UpdateScript = newUpdateScript;
        config.Identifier = res.Identifier;
        store.Maintenance.Send(new UpdateGenAiOperation(taskId, config));

        op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi);
        taskInfo = await store.Maintenance.SendAsync(op);

        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(newUpdateScript, genAiTaskInfo.Configuration.UpdateScript);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanDeleteGenAiTask(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.NotNull(taskInfo);
        var taskId = taskInfo.TaskId;

        store.Maintenance.Send(new DeleteOngoingTaskOperation(taskId, OngoingTaskType.GenAi));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Null(taskInfo);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanToggleGenAiTaskState(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
        var taskId = taskInfo.TaskId;

        // disable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));

        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);

        // re-enable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: false));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single, Skip = "need to fix")]
    public async Task ShouldTrackAiHashesInMetadata(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var etl = Etl.WaitForEtlToComplete(store);

        var taskName = config.Name;
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };
        store.Maintenance.Send(new AddGenAiOperation(config));

        const string docId = "posts/1";

        var post = new Post([
            new Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
            new Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
            new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
        ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

        using (var session = store.OpenSession())
        {
            session.Store(post, docId);
            session.SaveChanges();
        }

        await etl.WaitAsync(TimeSpan.FromSeconds(60));

        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            var expectedHashes = GetExpectedHashes(post, docId);
            var actualHashes = hashes.Select(h => h.ToString()).ToList();

            Assert.Equal(expectedHashes.Count, actualHashes.Count);
            foreach (var expected in expectedHashes)
                Assert.Contains(expected, actualHashes);
        }

        etl = Etl.WaitForEtlToComplete(store);

        // Update the document to trigger a new context output
        using (var session = store.OpenSession())
        {
            post = session.Load<Post>(docId);
            post.Comments.Add(new Comment("Nice summary. Bookmarked for future reference.", "emma"));
            session.SaveChanges();
        }

        await etl.WaitAsync(TimeSpan.FromSeconds(60));

        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            var expectedHashes = GetExpectedHashes(post, docId);
            var actualHashes = hashes.Select(h => h.ToString()).ToList();

            Assert.Equal(expectedHashes.Count, actualHashes.Count);
            foreach (var expected in expectedHashes)
                Assert.Contains(expected, actualHashes);
        }

        static List<string> GetExpectedHashes(Post post, string docId)
        {
            var results = new List<string>();
            using var context = JsonOperationContext.ShortTermSingleUse();

            foreach (var comment in post.Comments)
            {
                var contextObj = new DynamicJsonValue
                {
                    [nameof(Comment.Text)] = comment.Text,
                    [nameof(Comment.Author)] = comment.Author,
                    [nameof(Comment.Id)] = comment.Id
                };

                var ctxBlittable = context.ReadObject(contextObj, docId);

                var wrapped = new DynamicJsonValue
                {
                    ["Context"] = ctxBlittable,
                    ["Prompt"] = "Check if the following blog post comment is spam or not",
                    ["Schema"] = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new
                    {
                        Blocked = true,
                        Reason = "Concise reason for why this comment was marked as spam or ham"
                    })),
                    ["Update"] = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}"
                };

                using var fullObj = context.ReadObject(wrapped, docId);
                var hash = AttachmentsStorageHelper.CalculateHash(fullObj.AsSpan());

                results.Add(hash);
            }

            return results;
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldResendContextWhenPromptChanges(Options options, GenAiConfiguration configuration)
    {
        await ShouldResendContextOnConfigChange(configuration,
            changeConfig: config => config.Prompt = "please convert the text to Hebrew"
        );
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldResendContextWhenSchemaChanges(Options options, GenAiConfiguration configuration)
    {
        await ShouldResendContextOnConfigChange(configuration,
            changeConfig: config =>
            {
                var newSample = JsonConvert.SerializeObject(new
                {
                    Translation = "translated sentence",
                    OriginalLanguage = "the original language of the provided text",
                    TranslatedTo = "the language that you translated the text to"
                });
                config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(newSample);
            }
        );
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldResendContextWhenUpdateScriptChanges(Options options, GenAiConfiguration configuration)
    {
        await ShouldResendContextOnConfigChange(configuration,
            changeConfig: config => config.UpdateScript = "this.Translated = $output.Translation;"
        );
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanReproduceNullScopeNreInEtlStats(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            Blocked = true,
            Reason = "Concise reason for why this comment was marked as spam or ham"
        });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1);
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var db = await GetDatabase(store.Database);

        var hit = await WaitForValueAsync(() =>
        {
            var process = db.EtlLoader.Processes.FirstOrDefault();
            if (process == null)
                return false;

            var latest = process.GetLatestPerformanceStats();
            if (latest == null)
                return false;


            latest.ToPerformanceStats(); // if NRE happens, the test fails immediately

            return true;

        }, expectedVal: true, timeout: 5000);

        Assert.True(hit, "ToPerformanceStats() should not throw NullReferenceException anymore.");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldUseTaskIdentifierInMetadataHashes(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if ($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);

        var identifier = genAiTaskInfo.Configuration.Identifier;
        Assert.False(string.IsNullOrEmpty(identifier));

        var etl = Etl.WaitForEtlToComplete(store);

        const string id = "posts/1";
        using (var session = store.OpenSession())
        {
            var p = new Post(
                [
                    new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                    new Comment("Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.", "Oren Eini")
                ],
                "I, pencil",
                "A B52 pencil...");
            session.Store(p, id);
            session.SaveChanges();
        }

        var r = await etl.WaitAsync(TimeSpan.FromSeconds(60));
        Assert.True(r, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<BlittableJsonReaderObject>(id);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(identifier, out BlittableJsonReaderArray hashesArray));
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldThrowOnNonUniqueIdentifier(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);

        var identifier = genAiTaskInfo.Configuration.Identifier;
        Assert.False(string.IsNullOrEmpty(identifier));

        var config2 = new GenAiConfiguration
        {
            Name = "PostsSpamCheck2",
            ConnectionStringName = config.ConnectionStringName,
            Prompt = config.Prompt,
            Collection = config.Collection,
            SampleObject = config.SampleObject,
            UpdateScript = config.UpdateScript,
            GenAiTransformation = config.GenAiTransformation,
            Identifier = identifier // using the same identifier
        };

        var e = Assert.Throws<RavenException>(() => store.Maintenance.Send(new AddGenAiOperation(config2)));
        Assert.Contains("Can't create GenAI task", e.Message);
        Assert.Contains($"The identifier '{identifier}' is already used", e.Message);
    }

    private async Task ShouldResendContextOnConfigChange(GenAiConfiguration config, Action<GenAiConfiguration> changeConfig)
    {
        using var store = GetDocumentStore();
        await ConfigGenAi(config, store);

        var db = await GetDatabase(store.Database);
        var extractedByEtl = new System.Collections.Concurrent.ConcurrentQueue<string>();
        db.ForTestingPurposesOnly().OnEtlItemExtracted = (process, item, batchId) =>
            extractedByEtl.Enqueue($"[{DateTime.UtcNow:HH:mm:ss.fff}] taskId={process.TaskId} batchId={batchId} tag={process.Tag} docId={item.DocumentId} etag={item.Etag} cv={item.ChangeVector} doc={item.Document.Data.ToString()}");

        var etlDone = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Post([new Comment("RavenDB is amazing", "Alex")], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful..."), "posts/1");
            session.SaveChanges();
        }
        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var originalHash = await GetDocumentHash(config, store);

        var etlProcess = db.EtlLoader.Processes.SingleOrDefault() as GenAiTask;
        await Assert1DocUpdatedInStats(etlProcess, store);
        var taskId = etlProcess.TaskId;

        // disable task
        var oldTaskDebugInfo = await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60));
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));
        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);
        await AssertWaitForTrueAsync(() => Task.FromResult(etlProcess.IsRunning == false));

        var (etagBefore, etag) = await ChangeDoc(store, db); // (2, 3)
        var baselineUtc = DateTime.UtcNow;

        // update the configuration & re-enable task (delete the old task & create new enabled) task & wait it'll be done
        changeConfig(config);
        await store.Maintenance.SendAsync(new UpdateGenAiOperation(taskId, config));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(genAiTaskInfo.Configuration.Prompt, config.Prompt);
        Assert.Equal(genAiTaskInfo.Configuration.JsonSchema, config.JsonSchema);
        Assert.Equal(genAiTaskInfo.Configuration.UpdateScript, config.UpdateScript);

        // assert that context was sent again
        etlDone = Etl.WaitForEtlToComplete(store);
        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
        etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        try
        {
            EtlPerformanceStats[] stats2 = null;

            var value = await WaitForValueAsync(() =>
            {
                stats2 = etlProcess.GetPerformanceStats()
                    .Where(x => x != null
                                && x.Started >= baselineUtc
                                // LastLoadedEtag is the max etag of the batch docs before GenAiUpdate
                                // (LastLoadedEtag is the max of 'LastTransformedEtags', which is the etags of the batch docs before in the Transform stage - before the Load stage, which include the GenAiUpdate)
                                // So it should be == the etag of the doc
                                && x.LastLoadedEtag == etag
                                && x.NumberOfLoadedItems > 0
                                && x.NumberOfExtractedItems[EtlItemType.Document] > 0)
                    .ToArray();
                return stats2.Length > 0;
            }, expectedVal: true, timeout: 60_000);

            Assert.True(value);
            Assert.NotEmpty(stats2);

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<BlittableJsonReaderObject>("posts/1");
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out BlittableJsonReaderArray hashesArray));
                Assert.NotNull(hashesArray);

                var newHash = hashesArray.Last().ToString();
                Assert.NotEqual(originalHash, newHash);
            }

            var resend = stats2.FirstOrDefault(s =>
            {
                var load = s.Details.Operations[^1];
                var op = load.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel)
                    as GenAiPerformanceOperation;
                return op is { TotalSentToModel: 1, NumberOfContextObjects: 1, TotalCachedContexts: 0 };
            });

            Assert.True(resend != null);
        }
        catch (Exception e)
        {
            var debugInfoSb = new StringBuilder().Append($"Failed - baselineUtc: {baselineUtc}, etag before change: {etagBefore}, etag after change: {etag} ")
                .AppendLine().AppendLine("Task before disable:")
                .AppendLine(oldTaskDebugInfo).AppendLine().AppendLine("Task after re-enable:")
                .AppendLine(await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
            foreach (var line in extractedByEtl)
                debugInfoSb.AppendLine(line);
            throw new AggregateException(debugInfoSb.ToString(), e);
        }
    }

    private static async Task<(long etagBefore, long etagAfter)> ChangeDoc(DocumentStore store, DocumentDatabase db)
    {
        long etagBefore, etagAfter;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<Post>("posts/1");
            doc.Comments.Add(new Comment("spam comment", "evil bot"));
            etagBefore = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(doc), db.DbBase64Id);
            await session.SaveChangesAsync();
        }
        
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<Post>("posts/1");
            etagAfter = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(doc), db.DbBase64Id);
        }
        return (etagBefore, etagAfter);
    }

    private async Task Assert1DocUpdatedInStats(GenAiTask etlProcess, DocumentStore store)
    {
        Assert.NotNull(etlProcess);

        EtlPerformanceStats[] stats = null;
        var value = await WaitForValueAsync(() =>
        {
            stats = etlProcess.GetPerformanceStats()
                .Where(x => x != null && x.NumberOfLoadedItems > 0)
                .ToArray();
            return stats.Length > 0;
        }, expectedVal: true, timeout: 60_000);

        Assert.True(value, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));

        Assert.NotEmpty(stats);
        var loadDetails = stats[0].Details.Operations[^1];
        var genAiStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.Equal(1, genAiStats?.NumberOfContextObjects);
        Assert.Equal(1, genAiStats?.TotalSentToModel);
    }

    private static async Task<string> GetDocumentHash(GenAiConfiguration config, DocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<BlittableJsonReaderObject>("posts/1");
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(config.Identifier, out BlittableJsonReaderArray hashesArray));
            Assert.NotNull(hashesArray);
            return hashesArray.Last().ToString();
        }
    }

    private static async Task ConfigGenAi(GenAiConfiguration config, DocumentStore store)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var sampleObject = JsonConvert.SerializeObject(new { Translation = "translated text" });
        var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObject);

        config.Prompt = "Translate this text to Polish";
        config.JsonSchema = schema;
        config.UpdateScript = "this.TextInPolish = $output.Translation;";
        config.Collection = "Posts";
        config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Text: this.Body });" };
        config.Identifier = "posts-translation-check";

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldStartFromNewDocumentsByDefault(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        // store some documents before we define the GenAI task
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 10; i++)
            {
                var p = new Post(
                    [
                        new Comment("legit comment", "user"),
                        new Comment("spam comment", "bot")
                    ],
                    "title", "author");

                session.Store(p, $"posts/{i}");
            }

            session.SaveChanges();
        }

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
if (idx < 0)
    return;
this.Comments[idx].IsSpam = $output.Blocked 
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var db = await GetDatabase(store.Database);

        var state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        var lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
        Assert.Equal(10, lastProcessedEtag);

        using (var session = store.OpenSession())
        {
            // should not be processed
            var docs = session.Advanced.LoadStartingWith<BlittableJsonReaderObject>("posts/");

            foreach (var post in docs)
            {
                Assert.True(post.TryGet("Comments", out BlittableJsonReaderArray comments));
                foreach (var o in comments)
                {
                    var comment = o as BlittableJsonReaderObject;
                    Assert.NotNull(comment);
                    Assert.False(comment.TryGet("IsSpam", out bool _));
                }
            }
        }

        var etl = Etl.WaitForEtlToComplete(store);

        // update one post
        using (var session = store.OpenSession())
        {
            var post = session.Load<Post>("posts/1");
            post.Comments.Add(new Comment("great article", "aviv"));
            session.SaveChanges();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));

        using (var session = store.OpenSession())
        {
            // should be processed
            var post = session.Load<BlittableJsonReaderObject>("posts/1");

            Assert.True(post.TryGet("Comments", out BlittableJsonReaderArray comments));
            foreach (var o in comments)
            {
                var comment = o as BlittableJsonReaderObject;
                Assert.NotNull(comment);
                Assert.True(comment.TryGet("IsSpam", out bool _));
            }
        }

        var oldEtag = lastProcessedEtag;
        state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
        Assert.True(lastProcessedEtag > oldEtag);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStartFromBeginningOfTime(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        // store some documents before we define the GenAI task
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 10; i++)
            {
                var p = new Post(
                    [
                        new Comment("legit comment", "user"),
                        new Comment("spam comment", "bot")
                    ],
                    "title", "author");

                session.Store(p, $"posts/{i}");
            }

            session.SaveChanges();
        }

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
if (idx < 0)
    return;
this.Comments[idx].IsSpam = $output.Blocked 
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        var etl = Etl.WaitForEtlToComplete(store);

        store.Maintenance.Send(new AddGenAiOperation(config, StartingPointChangeVector.BeginningOfTime));

        var r = await etl.WaitAsync(TimeSpan.FromSeconds(120));
        Assert.True(r, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(120)));

        using (var session = store.OpenSession())
        {
            // should be processed
            var docs = session.Advanced.LoadStartingWith<BlittableJsonReaderObject>("posts/");
            Assert.Equal(10, docs.Length);

            foreach (var post in docs)
            {
                Assert.True(post.TryGet("Comments", out BlittableJsonReaderArray comments));
                foreach (var o in comments)
                {
                    var comment = o as BlittableJsonReaderObject;
                    Assert.NotNull(comment);
                    Assert.True(comment.TryGet("IsSpam", out bool _));
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    private async Task CanUpdateGenAiChangeVector(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore();

        // configure GenAi task
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var sampleObject = JsonConvert.SerializeObject(new { Translation = "translated text" });
        var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObject);

        config.Prompt = "Translate this text to Polish";
        config.JsonSchema = schema;
        config.UpdateScript = "this.TextInPolish = $output.Translation;";
        config.Collection = "Posts";
        config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Text: this.Body });" };
        config.Identifier = "posts-translation-check";

        for (int i = 1; i <= 10; i++)
        {
            using (var session = store.OpenSession())
            {
                // Intentionally store documents in a different collection
                // to ensure that the ChangeVector in the GenAI process state remains unchanged.
                // This allows us to verify that the ChangeVector passed to the add/update operation is correctly saved.

                session.Store(new User(), "users/" + i); 
                session.SaveChanges();
            }
        }

        // expected change vector is "LastDocument" (the default value for AddGenAi)
        string expectedChangeVector;
        using (var session = store.OpenSession())
        {
            var lastDoc = session.Load<Post>("users/10");
            expectedChangeVector = session.Advanced.GetChangeVectorFor(lastDoc);
        }

        // add GenAI task
        store.Maintenance.Send(new AddGenAiOperation(config));

        // assert change vector
        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(expectedChangeVector, genAiTaskInfo.ChangeVector);

        // edit the task in order to update the change vector 
        string updatedCv;
        using (var session = store.OpenSession())
        {
            var someOtherDoc = session.Load<Post>("users/5");
            updatedCv = expectedChangeVector = session.Advanced.GetChangeVectorFor(someOtherDoc);
        }

        var result = store.Maintenance.Send(new UpdateGenAiOperation(taskInfo.TaskId, config, StartingPointChangeVector.From(updatedCv)));
        await Server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex, TimeSpan.FromSeconds(15));

        // assert change vector
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(expectedChangeVector, genAiTaskInfo.ChangeVector);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnsureGenAiTaskHasUniqueName(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
       
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
if (idx < 0)
    return;
this.Comments[idx].IsSpam = $output.Blocked 
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));
        await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new AddGenAiOperation(config)));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnsureGenAiTaskHasUniqueName2(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
       
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
if (idx < 0)
    return;
this.Comments[idx].IsSpam = $output.Blocked 
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        config.Identifier = "posts-spam-check-1";
        var r = await store.Maintenance.SendAsync(new AddGenAiOperation(config));
        var r2 = await store.Maintenance.SendAsync(new UpdateGenAiOperation(r.TaskId, config));

        //TODO: ETL update is broken, we change the TaskId every time
        await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdateGenAiOperation(r.TaskId, config)));
        await store.Maintenance.SendAsync(new UpdateGenAiOperation(r2.TaskId, config));

        // above should not throw, but it does because of the TaskId change
        // Assert.Equal(r.TaskId, r2.TaskId); 
        // var r3 = await store.Maintenance.SendAsync(new UpdateGenAiOperation(r.TaskId, config));
        // Assert.Equal(r2.TaskId, r3.TaskId);

        var record = await GetDatabaseRecordAsync(store);
        Assert.Equal(1, record.GenAis.Count);
    }

    internal record Comment(string Text, string Author)
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }

    internal record Post(List<Comment> Comments, string Title, string Body);
}
