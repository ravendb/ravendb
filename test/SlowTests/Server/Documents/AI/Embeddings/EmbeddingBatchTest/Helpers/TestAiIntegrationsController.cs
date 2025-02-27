using System.Collections.Generic;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;

public class TestAiIntegrationsController
{
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _services = new();

    public TestDocumentDatabaseStub Database { get; }

    public TestAiIntegrationsController(TestDocumentDatabaseStub database)
    {
        Database = database;
    }

    public void RegisterService(AiConnectionStringIdentifier id, ITextEmbeddingGenerationService service)
    {
        _services[id] = service;
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringId, out ITextEmbeddingGenerationService service)
    {
        return _services.TryGetValue(connectionStringId, out service);
    }
}
