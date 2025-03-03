using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
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
    protected readonly TimeSpan DefaultEtlTimeout = TimeSpan.FromSeconds(30);
    protected static readonly ChunkingOptions DefaultChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 };
    
    protected float[] GenerateEmbeddingForTextViaOnnx(string text)
    {
        _allocator ??= new(SharedMultipleUseFlag.None);
        return MemoryMarshal.Cast<byte, float>(GenerateEmbeddings.FromText(_allocator, VectorOptions.DefaultText, text).GetEmbedding()).ToArray();
    }

    protected static (EmbeddingsGenerationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) RegisterAiIntegration(
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
            TargetQuantizationType = targetQuantization,
            ChunkingOptionsForQuerying = chunkingOptionsForQuerying ?? DefaultChunkingOptions,
        };

        configuration.Identifier = configuration.GenerateIdentifier();

        return RegisterAiIntegration(store, configuration);
    }

    protected static (EmbeddingsGenerationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) RegisterAiIntegration(
        IDocumentStore store, EmbeddingsGenerationConfiguration configuration)
    {
        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, OnnxSettings = new OnnxSettings() };

        connectionString.Identifier = connectionString.GenerateIdentifier();

        var putResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        store.Maintenance.Send(new AddAiIntegrationOperation(configuration));

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
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        var source = session.Load<object>(docId);
        Assert.NotNull(source);

        foreach (var inputValue in inputValues)
        {
            //Assert if value is in embedding cache
            var hashOfInput = EmbeddingsHelper.CalculateInputValueHash(inputValue);
            //todo maciej
            var embeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hashOfInput, targetQuantization);
            var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
            Assert.NotNull(embeddingCacheDocument);

            var attachmentsExistsInEmbeddingCache = session.Advanced.Attachments.Exists(embeddingsDocumentId, hashOfInput);
            Assert.True(attachmentsExistsInEmbeddingCache);

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

            // Assert if current path contain embedding of current input value
            var expectedAttachmentNameInEmbeddingsDocument = EmbeddingsHelper.GenerateDestinationAttachmentName(EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(integrationIdentifier, path),hashOfInput, targetQuantization);
            var attachmentsByEtlPath = currentPathObject.Select(att => att.ToString()).ToList();
            Assert.Equal(inputValues.Length, attachmentsByEtlPath.Count); // <- this checks if we've all embeddings
            Assert.Contains(expectedAttachmentNameInEmbeddingsDocument, attachmentsByEtlPath);

            // Assert if the referenced document exists
            var attachmentExistsInEmbeddingsDocument = session.Advanced.Attachments.Exists(documentEmbeddingsId, expectedAttachmentNameInEmbeddingsDocument);
            Assert.True(attachmentExistsInEmbeddingsDocument);
        }
    }

    public override void Dispose()
    {
        _allocator?.Dispose();
        base.Dispose();
    }
}
