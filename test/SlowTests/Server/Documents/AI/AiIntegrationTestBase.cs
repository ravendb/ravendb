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
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI;

public abstract class AiIntegrationTestBase(ITestOutputHelper output) : RavenTestBase(output)
{
    protected const string DefaultConnectionStringName = "Local AI connection";
    protected const string DefaultAiIntegrationTaskName = "localAiTask";
    protected ByteStringContext _allocator;

    protected float[] GenerateEmbeddingForTextViaOnnx(string text)
    {
        _allocator ??= new(SharedMultipleUseFlag.None);
        return MemoryMarshal.Cast<byte, float>(GenerateEmbeddings.FromText(_allocator, VectorOptions.DefaultText, text).GetEmbedding()).ToArray();
    }

    protected static (AiIntegrationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) RegisterAiIntegration(
        IDocumentStore store,
        string aiIntegrationName = DefaultAiIntegrationTaskName,
        string connectionStringName = DefaultConnectionStringName,
        List<string> embeddingsPaths = null,
        string script = null,
        string collectionName = null)
    {
        var configuration = new AiIntegrationConfiguration
        {
            Name = aiIntegrationName,
            ConnectionStringName = connectionStringName,
            EmbeddingsPaths = embeddingsPaths ?? (string.IsNullOrEmpty(script) ? ["Name"] : null),
            Collection = collectionName ?? "Dtos",
            EmbeddingsTransformation = string.IsNullOrEmpty(script) == false ? new AiEmbeddingsTransformation
            {
                Script = script
            }
            : null,
        };

        configuration.Identifier = configuration.GenerateIdentifier();

        return RegisterAiIntegration(store, configuration);
    }

    protected static (AiIntegrationConfiguration AiIntegrationConfiguration, AiConnectionString connectionString) RegisterAiIntegration(
        IDocumentStore store, AiIntegrationConfiguration configuration)
    {
        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, OnnxSettings = new OnnxSettings() };

        connectionString.Identifier = connectionString.GenerateIdentifier();

        var putResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        store.Maintenance.Send(new AddAiIntegrationOperation(configuration));

        return (configuration, connectionString);
    }

    protected void AssertEmbeddingsForPath(
        IDocumentStore store,
        AiIntegrationIdentifier integrationIdentifier,
        AiConnectionStringIdentifier connectionStringIdentifier,
        string path,
        string[] inputValues,
        string docId)
    {
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        var source = session.Load<object>(docId);
        Assert.NotNull(source);

        foreach (var inputValue in inputValues)
        {
            //Assert if value is in embedding cache
            var hashOfInput = AiHelper.CalculateValueHash(inputValue);
            var embeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(connectionStringIdentifier, hashOfInput);
            var embeddingCacheDocument = session.Load<object>(embeddingsDocumentId) as JObject;
            Assert.NotNull(embeddingCacheDocument);

            //Assert if current key is properly persisted with an embedding 
            var sourceAttachmentName = embeddingCacheDocument[inputValue]?.ToString();
            Assert.NotNull(sourceAttachmentName); // Checks if current embedding cache has the embedding
            var attachmentsExistsInEmbeddingCache = session.Advanced.Attachments.Exists(embeddingsDocumentId, sourceAttachmentName);
            Assert.True(attachmentsExistsInEmbeddingCache);

            //Assert if embeddings document exists
            var documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(docId);
            var documentEmbeddings = session.Load<object>(documentEmbeddingsId) as JObject;
            Assert.NotNull(documentEmbeddings);

            // Assert if contains current ETL result
            var currentEtlObject = documentEmbeddings[integrationIdentifier.Value];
            Assert.NotNull(currentEtlObject);

            // Assert if ETL result contains current path
            var currentPathObject = currentEtlObject[path] as JArray;
            Assert.NotNull(currentPathObject);

            // Assert if current path contain embedding of current input value
            var expectedAttachmentNameInEmbeddingsDocument = AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(integrationIdentifier, path) + sourceAttachmentName;
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
