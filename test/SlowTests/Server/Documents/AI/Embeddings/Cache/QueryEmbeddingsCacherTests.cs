using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.Cache;

public class QueryEmbeddingsCacherTests : RavenLowLevelTestBase
{
    public QueryEmbeddingsCacherTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CacherShouldNotBeRunningIfThereIsNoEmbeddingsGenerationTask()
    {
        using var db = CreateDocumentDatabase();

        var cacher = db.AiIntegrations.Embeddings.QueryEmbeddingsCacher;

        Assert.False(cacher.IsRunning);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void ShouldCacheEmbeddings()
    {
        using var db = CreateDocumentDatabase();

        var aiCs = new AiConnectionStringIdentifier("embeddings-gen-connection");
        
        var cacher = db.AiIntegrations.Embeddings.QueryEmbeddingsCacher;

        var expireAt = db.Time.GetUtcNow().AddDays(7);

        var embedding1 = new EmbeddingGenerationItem("test1", MemoryMarshalEx.Cast<float,byte>(new []{0.1f, 0.2f, 0.3f}), VectorEmbeddingType.Single, aiCs)
        {
            ExpireAt = expireAt
        };
        var embedding2 = new EmbeddingGenerationItem("test2",MemoryMarshalEx.Cast<float,byte>(new[]{0.3f, 0.5f, 1.1f}), VectorEmbeddingType.Single, aiCs)
        {
            ExpireAt = expireAt
        };

        cacher.EnqueueEmbeddingsToCache([embedding1, embedding2]);

        cacher.CacheEnqueuedEmbeddings();

        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var embedding in new[]{embedding1, embedding2})
            {
                var result = db.AiIntegrations.Embeddings.Storage.TryGetEmbeddingCacheDocument(context, aiCs, embedding.ValueHash, VectorEmbeddingType.Single, out string id,
                    out _);

                Assert.True(result);

                Assert.Equal(EmbeddingsHelper.GetEmbeddingCacheDocumentId(aiCs, embedding.ValueHash, VectorEmbeddingType.Single), id);

                var attachment = db.DocumentsStorage.AttachmentsStorage.GetAttachment(context, id, embedding.ValueHash, AttachmentType.Document, null);

                Assert.NotNull(attachment);
                
                Assert.Equal(embedding.ValueHash, attachment.Name);
                Assert.Equal(12, attachment.Size);
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void ShouldCacheEmbeddingsInMultipleBatches()
    {
        using var db = CreateDocumentDatabase();

        var aiCs = new AiConnectionStringIdentifier("embeddings-gen-connection");

        var cacher = db.AiIntegrations.Embeddings.QueryEmbeddingsCacher;

        var expireAt = db.Time.GetUtcNow().AddDays(7);

        var embeddings = new List<EmbeddingGenerationItem>();

        for (int i = 0; i < db.Configuration.Ai.QueryEmbeddingsGenerationMaxCacheBatchSize * 2 + 5; i++)
        {
            var embedding = new EmbeddingGenerationItem("test" + i, MemoryMarshalEx.Cast<float,byte>(new[]{0.1f, 0.2f, 0.3f, i}), VectorEmbeddingType.Single, aiCs)
            {
                ExpireAt = expireAt
            };
            
            embeddings.Add(embedding);
        }

        cacher.EnqueueEmbeddingsToCache(embeddings);

        for (int i = 0; i < 3; i++)
        {
            var hasMore = cacher.CacheEnqueuedEmbeddings();
            
            if (i < 2)
                Assert.True(hasMore);
            else
                Assert.False(hasMore);
        }
        
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var embedding in embeddings)
            {
                var result = db.AiIntegrations.Embeddings.Storage.TryGetEmbeddingCacheDocument(context, aiCs, embedding.ValueHash, VectorEmbeddingType.Single, out string id,
                    out _);

                Assert.True(result);

                Assert.Equal(EmbeddingsHelper.GetEmbeddingCacheDocumentId(aiCs, embedding.ValueHash, VectorEmbeddingType.Single), id);

                var attachment = db.DocumentsStorage.AttachmentsStorage.GetAttachment(context, id, embedding.ValueHash, AttachmentType.Document, null);

                Assert.NotNull(attachment);

                Assert.Equal(embedding.ValueHash, attachment.Name);
                Assert.Equal(16, attachment.Size);
            }
        }
    }
}
