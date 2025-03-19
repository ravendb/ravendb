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
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
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

        EmbeddingsGenerationTaskIdentifier taskId = new("local-gen");
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext operationContext))
        {
            operationContext.OpenReadTransaction();
            var valueTask1 = db.EmbeddingsGeneratorEtl.GetEmbeddingsForQueryAsync(operationContext, taskId, "test1");
            var valueTask2 = db.EmbeddingsGeneratorEtl.GetEmbeddingsForQueryAsync(operationContext, taskId, "test2");
            var valueTask3= db.EmbeddingsGeneratorEtl.GetEmbeddingsForQueryAsync(operationContext, taskId, "test2");
            await Task.WhenAll(valueTask1.AsTask(), valueTask2.AsTask());
        }
        
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext operationContext))
        {
            operationContext.OpenReadTransaction();
            var valueTask1 = db.EmbeddingsGeneratorEtl.GetEmbeddingsForQueryAsync(operationContext, taskId,  "test1");
            Assert.True(valueTask1.IsCompleted);
            var valueTask2 = db.EmbeddingsGeneratorEtl.GetEmbeddingsForQueryAsync(operationContext, taskId,  "test2");
            Assert.True(valueTask2.IsCompleted);
        }
    }
}
