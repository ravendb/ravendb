using System;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Sparrow.Server.Logging;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _embeddingsGenerationServiceByConnectionStringIdentifier;

    private Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTaskIdentifiers;
    private Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingsGenerationConfigurationByTaskIdentifiers;

    private readonly RavenLogger _logger;
    
    public DocumentDatabase Database { get; }

    public AiIntegrationsController(DocumentDatabase database)
    {
        Database = database;
        _embeddingsGenerationServiceByConnectionStringIdentifier = new();
        _embeddingsGenerationConfigurationByTaskIdentifiers = new();
        _connectionStringsByTaskIdentifiers = new();

        var storage = new EmbeddingsStorage(database);
        var cacher = new QueryEmbeddingsCacher(database, database.DatabaseShutdown);

        Embeddings = new EmbeddingsController(this, storage, cacher);

        _logger = database.Loggers.GetLogger<AiIntegrationsController>();
    }

    public EmbeddingsController Embeddings { get; private set; }

    public bool TryGetEmbeddingsGenerationConfiguration(EmbeddingsGenerationTaskIdentifier taskIdentifier, out EmbeddingsGenerationConfiguration configuration)
    {
        return _embeddingsGenerationConfigurationByTaskIdentifiers.TryGetValue(taskIdentifier, out configuration);
    }

    public bool TryGetConnectionStringIdByEmbeddingsGenerationTask(EmbeddingsGenerationTaskIdentifier taskIdentifier, out AiConnectionStringIdentifier connectionString)
    {
        return _connectionStringsByTaskIdentifiers.TryGetValue(taskIdentifier, out connectionString);
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;
        
        var embeddingsGenerationServiceByConnectionStringIdentifier = new Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService>();
        var connectionStringsByTaskIdentifier = new Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier>();
        var embeddingsGenerationConfigurationsByTaskIdentifier = new Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration>();

        foreach (var connectionStringKvp in record.AiConnectionStrings)
        {
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionStringKvp.Value.Identifier);
            var connectionString = connectionStringKvp.Value;

            if (_embeddingsGenerationServiceByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out var embeddingsGenerationService) == false)
                embeddingsGenerationService = AiHelper.CreateService(connectionString);

            embeddingsGenerationServiceByConnectionStringIdentifier[connectionStringIdentifier] = embeddingsGenerationService;
        }

        var hasTasks = false;

        foreach (var embeddingGenerationConfiguration in record.EmbeddingsGenerations)
        {
            hasTasks = true;

            var embeddingsGeneratorIdentifier = new EmbeddingsGenerationTaskIdentifier(embeddingGenerationConfiguration.Identifier);
            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[embeddingGenerationConfiguration.ConnectionStringName].Identifier);

            connectionStringsByTaskIdentifier[embeddingsGeneratorIdentifier] = connectionStringIdentifier;

            embeddingsGenerationConfigurationsByTaskIdentifier[embeddingsGeneratorIdentifier] = embeddingGenerationConfiguration;
        }

        // TODO
        //Embeddings.UpdateBatchingWorkerForConnectionStringIdAsync()
        //Embeddings.RemoveBatchingWorkerForConnectionStringIdAsync()

        _embeddingsGenerationServiceByConnectionStringIdentifier = embeddingsGenerationServiceByConnectionStringIdentifier;
        _connectionStringsByTaskIdentifiers = connectionStringsByTaskIdentifier;
        _embeddingsGenerationConfigurationByTaskIdentifiers = embeddingsGenerationConfigurationsByTaskIdentifier;

        if (Embeddings.QueryEmbeddingsCacher.IsRunning)
        {
            if (hasTasks == false)
                Embeddings.QueryEmbeddingsCacher.Stop();
        }
        else
        {
            if (hasTasks)
                Embeddings.QueryEmbeddingsCacher.Start();
        }
    }

    public void Dispose()
    {
        Embeddings.QueryEmbeddingsCacher.Dispose();
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringIdentifier, out ITextEmbeddingGenerationService service)
    {
        return _embeddingsGenerationServiceByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out service);
    }

    public void Initialize(DatabaseRecord record)
    {
        HandleDatabaseRecordChange(record);
    }
}
