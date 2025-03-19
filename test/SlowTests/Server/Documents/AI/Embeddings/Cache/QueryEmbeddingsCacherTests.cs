using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Azure;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.Cache;

public class QueryEmbeddingsCacherTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public async Task ShouldCacheEmbeddings()
    {
        var store = GetDocumentStore();
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "local-embedder", Identifier = "local", EmbeddedSettings = new()
        }));

        await store.Maintenance.SendAsync(new AddEtlOperation<AiConnectionString>(new EmbeddingsGenerationConfiguration
        {
            Identifier = "local-gen",
            Name = "Local embedding gen",
            Collection = "Users",
            ConnectionStringName = "local-embedder",
            ChunkingOptionsForQuerying = new ChunkingOptions { MaxTokensPerChunk = 256 },
            EmbeddingsPathConfigurations =
            [
                new EmbeddingPathConfiguration
                {
                    Path = "Name", ChunkingOptions = new ChunkingOptions { MaxTokensPerChunk = 256, ChunkingMethod = ChunkingMethod.PlainTextSplit }
                }
            ]
        }));

        var db = await GetDatabase(store.Database);
        
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext operationContext))
        {
            operationContext.OpenReadTransaction();
            List<Task> tasks = [];
            var cached = db.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(operationContext, new("local-gen"), "test1", ref tasks);
            Assert.False(cached);
            cached = db.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(operationContext, new("local-gen"), "test2", ref tasks);
            Assert.False(cached);
            cached = db.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(operationContext, new("local-gen"), "test2", ref tasks);
            if (cached is false) // race condition - we may have computed the test2 embedding+store
            {
                // but if we didn't, we should get the same task, since we'll only compute "test2" once
                Assert.Same(tasks[^1], tasks[^2]);
            }

            Task.WaitAll(tasks.ToArray());
        }
        
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext operationContext))
        {
            operationContext.OpenReadTransaction();
            List<Task> tasks = [];
            var cached = db.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(operationContext, new("local-gen"), "test1", ref tasks);
            Assert.True(cached);
            cached = db.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(operationContext, new("local-gen"), "test2", ref tasks);
            Assert.True(cached);
        }
    }
}
