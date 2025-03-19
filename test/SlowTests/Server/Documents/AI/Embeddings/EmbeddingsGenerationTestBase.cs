using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions.Streams;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public abstract class EmbeddingsGenerationTestBase(ITestOutputHelper output) : RavenTestBase(output)
{
    protected const string DefaultConnectionStringName = "Local AI connection";
    protected const string DefaultEmbeddingGenerationTaskName = "localAiTask";
    protected ByteStringContext _allocator;
    protected readonly TimeSpan DefaultEtlTimeout = Debugger.IsAttached == false ? TimeSpan.FromSeconds(30) : TimeSpan.FromMinutes(15);

    protected static readonly ChunkingOptions DefaultChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 };
    
    protected float[] GenerateEmbeddingForTextViaOnnx(string text)
    {
        _allocator ??= new(SharedMultipleUseFlag.None);
        return MemoryMarshal.Cast<byte, float>(GenerateEmbeddings.FromText(_allocator, VectorOptions.DefaultText, text).GetEmbedding()).ToArray();
    }

    protected static (EmbeddingsGenerationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) AddEmbeddingsGenerationTask(
        IDocumentStore store,
        string embeddingsGenerationTaskName = DefaultEmbeddingGenerationTaskName,
        string connectionStringName = DefaultConnectionStringName,
        List<EmbeddingPathConfiguration> embeddingsPaths = null,
        string script = null,
        string collectionName = null,
        VectorEmbeddingType targetQuantization = VectorEmbeddingType.Single,
        ChunkingOptions chunkingOptionsForQuerying = null)
    {
        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = embeddingsGenerationTaskName,
            ConnectionStringName = connectionStringName,
            EmbeddingsPathConfigurations = embeddingsPaths ?? (string.IsNullOrEmpty(script) ? [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }] : null),
            Collection = collectionName ?? "Dtos",
            EmbeddingsTransformation = string.IsNullOrEmpty(script) == false ? new EmbeddingsTransformation
            {
                Script = script
            }
            : null,
            Quantization = targetQuantization,
            ChunkingOptionsForQuerying = chunkingOptionsForQuerying ?? DefaultChunkingOptions,
        };

        configuration.Identifier = configuration.GenerateIdentifier();

        return AddEmbeddingsGenerationTask(store, configuration);
    }

    protected static (EmbeddingsGenerationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) AddEmbeddingsGenerationTask(
        IDocumentStore store, EmbeddingsGenerationConfiguration configuration)
    {
        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, EmbeddedSettings = new EmbeddedSettings() };

        connectionString.Identifier = connectionString.GenerateIdentifier();

        var putResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));

        return (configuration, connectionString);
    }
    
    protected void AssertEmbeddingsForPath(IDocumentStore store,
        EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration,
        AiConnectionString aiConnectionString,
        string path,
        string[] inputValues,
        string docId,
        VectorEmbeddingType targetQuantization = VectorEmbeddingType.Single) => AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationConfiguration.Identifier), new AiConnectionStringIdentifier(aiConnectionString.Identifier), path, inputValues, docId, targetQuantization);

    protected void AssertEmbeddingsForPath(
        IDocumentStore store,
        EmbeddingsGenerationTaskIdentifier integrationIdentifier,
        AiConnectionStringIdentifier connectionStringIdentifier,
        string path,
        string[] inputValues,
        string docId,
        VectorEmbeddingType targetQuantization = VectorEmbeddingType.Single)
    {
        AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, path, inputValues, docId, targetQuantization, assertMissing: false);
    }
    protected void AssertMissingEmbeddingsForPath(
        IDocumentStore store,
        EmbeddingsGenerationTaskIdentifier integrationIdentifier,
        AiConnectionStringIdentifier connectionStringIdentifier,
        string path,
        string[] inputValues,
        string docId,
        VectorEmbeddingType targetQuantization = VectorEmbeddingType.Single)
    {
        AssertEmbeddingsForPath(store, integrationIdentifier, connectionStringIdentifier, path, inputValues, docId, targetQuantization, assertMissing: true);
    }
    protected void AssertEmbeddingsForPath(
        IDocumentStore store,
        EmbeddingsGenerationTaskIdentifier integrationIdentifier,
        AiConnectionStringIdentifier connectionStringIdentifier,
        string path,
        string[] inputValues,
        string docId,
        VectorEmbeddingType targetQuantization,
        bool assertMissing)
    {
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        var source = session.Load<object>(docId);
        Assert.NotNull(source);

        foreach (var inputValue in inputValues)
        {
            //Assert if value is in embedding cache
            var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(inputValue);
            var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput, targetQuantization);
            var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
            Assert.NotNull(embeddingCacheDocument);

            var attachmentsInEmbeddingCache = session.Advanced.Attachments.Get(embeddingsDocumentId, hashOfInput);
            Assert.NotNull(attachmentsInEmbeddingCache);
            var hashContentHash = AttachmentsStorageHelper.CalculateHash(attachmentsInEmbeddingCache.Stream.ReadData());

            //Assert if embeddings document exists
            var documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(docId);
            var documentEmbeddings = session.Load<object>(documentEmbeddingsId) as JObject;
            Assert.NotNull(documentEmbeddings);

            // Assert if contains current ETL result
            var currentEtlObject = documentEmbeddings[integrationIdentifier.Value];
            Assert.NotNull(currentEtlObject);

            // Assert if ETL result contains current path
            var currentPathObject = currentEtlObject[path] as JArray;
            Assert.NotNull(currentPathObject);

            var attachmentExistsInEmbeddingsDocument = session.Advanced.Attachments.Exists(documentEmbeddingsId, hashContentHash);
            if (assertMissing is false)
            {
                // Assert if current path contain embedding of current input value
                Assert.Contains(hashContentHash, currentPathObject.Select(x=>x.ToString()));
                Assert.True(attachmentExistsInEmbeddingsDocument);
            }
            else
            {
                Assert.DoesNotContain(hashContentHash, currentPathObject.Select(x=>x.ToString()));
                Assert.False(attachmentExistsInEmbeddingsDocument);
            }
        }
    }

    public override void Dispose()
    {
        _allocator?.Dispose();
        base.Dispose();
    }
}
