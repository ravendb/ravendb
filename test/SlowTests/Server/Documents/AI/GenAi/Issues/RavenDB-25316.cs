using System;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25316_GenAiValidation(ITestOutputHelper output) : RavenTestBase(output)
    {
        private static void SeedMinimalValidConfig(GenAiConfiguration cfg)
        {
            cfg.Name = "GenAi-Validation";
            cfg.Identifier = "genai-validation";
            cfg.Collection = "Docs";
            cfg.Prompt = "You are a helpful assistant.";
            cfg.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ text: this.Body });" };
            cfg.UpdateScript = "this.Answer = $output.Answer;";

            var sample = JsonConvert.SerializeObject(new { Answer = "text" });
            cfg.SampleObject = sample;
            cfg.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(sample);
        }

        private static long GetTaskId(IDocumentStore store, string name)
        {
            var info = store.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.GenAi));
            return info.TaskId;
        }

        private static void AssertWrappedInner(RavenException ex, params string[] substrings)
        {
            Assert.NotNull(ex);
            Assert.NotNull(ex.InnerException);
            var inner = Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("Invalid ETL configuration", inner.Message);
            Assert.Contains("Prompt", inner.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void AddGenAiShouldFailWhenPromptIsNullOrEmpty(Options options, GenAiConfiguration config, string promptValue)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;

            config.Prompt = promptValue;

            var ex = Assert.Throws<RavenException>(() =>
                store.Maintenance.Send(new AddGenAiOperation(config)));

            AssertWrappedInner(ex);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void UpdateGenAiShouldFailWhenPromptIsNullOrEmpty(Options options, GenAiConfiguration config, string promptValue)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;
            store.Maintenance.Send(new AddGenAiOperation(config));
            var taskId = GetTaskId(store, config.Name);

            config.Prompt = promptValue;

            var ex = Assert.Throws<RavenException>(() =>
                store.Maintenance.Send(new UpdateGenAiOperation(taskId, config)));

            AssertWrappedInner(ex);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void AddGenAiShouldSucceedWhenOnlySampleObjectIsProvided(Options options, GenAiConfiguration config, string jsonSchema)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;
            config.JsonSchema = jsonSchema;

            store.Maintenance.Send(new AddGenAiOperation(config));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void AddGenAiShouldSucceedWhenOnlyJsonSchemaIsProvided(Options options, GenAiConfiguration config, string sampleObject)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;

            config.SampleObject = sampleObject;
            
            store.Maintenance.Send(new AddGenAiOperation(config));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void UpdateGenAiShouldSucceedWhenOnlySampleObjectIsProvided(Options options, GenAiConfiguration config, string jsonSchema)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;
            store.Maintenance.Send(new AddGenAiOperation(config));
            var taskId = GetTaskId(store, config.Name);

            config.JsonSchema = jsonSchema;

            store.Maintenance.Send(new UpdateGenAiOperation(taskId, config));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void UpdateGenAiShouldSucceedWhenOnlyJsonSchemaIsProvided(Options options, GenAiConfiguration config, string sampleObject)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;
            store.Maintenance.Send(new AddGenAiOperation(config));
            var taskId = GetTaskId(store, config.Name);

            config.SampleObject = sampleObject;

            store.Maintenance.Send(new UpdateGenAiOperation(taskId, config));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [""])]
        public void UpdateGenAiShouldFailWhenBothSampleObjectAndJsonSchemaAreNull(Options options, GenAiConfiguration config, string emptyOrNull)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            SeedMinimalValidConfig(config);
            config.ConnectionStringName = config.Connection.Name;
            store.Maintenance.Send(new AddGenAiOperation(config));
            var taskId = GetTaskId(store, config.Name);

            config.JsonSchema = emptyOrNull;
            config.SampleObject = emptyOrNull;

            var ex = Assert.Throws<RavenException>(() =>
                store.Maintenance.Send(new UpdateGenAiOperation(taskId, config)));

            AssertWrappedInner(ex);
        }
    }
}
