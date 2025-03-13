using System;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _embeddingsGenerationServiceByConnectionStringIdentifier;

    private Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTaskIdentifiers;
    private Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingsGenerationConfigurationByTaskIdentifiers;

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
    }

    public EmbeddingsController Embeddings { get; private set; }

    public bool TryGetEmbeddingsGenerationConfiguration(EmbeddingsGenerationTaskIdentifier taskIdentifier, out EmbeddingsGenerationConfiguration configuration)
    {
        return _embeddingsGenerationConfigurationByTaskIdentifiers.TryGetValue(taskIdentifier, out configuration);
    }

    public bool TryGetConnectionStringByEmbeddingsGenerationTask(EmbeddingsGenerationTaskIdentifier taskIdentifier, out AiConnectionStringIdentifier connectionString)
    {
        return _connectionStringsByTaskIdentifiers.TryGetValue(taskIdentifier, out connectionString);
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

        var connectionStringsByTaskIdentifier = new Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier>();
        var embeddingsGenerationConfigurationsByTaskIdentifier = new Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration>();

        foreach (var connectionStringKvp in record.AiConnectionStrings)
        {
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionStringKvp.Value.Identifier);
            var connectionString = connectionStringKvp.Value;

            if (_embeddingsGenerationServiceByConnectionStringIdentifier.ContainsKey(connectionStringIdentifier))
                continue;

            _embeddingsGenerationServiceByConnectionStringIdentifier[connectionStringIdentifier] = AiHelper.CreateService(connectionString);
        }

        var numberOfEmbeddingGenerationTasks = 0;

        foreach (var embeddingGenerationConfiguration in record.EmbeddingsGenerations)
        { 
            numberOfEmbeddingGenerationTasks++;

            var embeddingsGeneratorIdentifier = new EmbeddingsGenerationTaskIdentifier(embeddingGenerationConfiguration.Identifier);
            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[embeddingGenerationConfiguration.ConnectionStringName].Identifier);

            connectionStringsByTaskIdentifier[embeddingsGeneratorIdentifier] = connectionStringIdentifier;

            embeddingsGenerationConfigurationsByTaskIdentifier[embeddingsGeneratorIdentifier] = embeddingGenerationConfiguration;
        }

        _connectionStringsByTaskIdentifiers = connectionStringsByTaskIdentifier;
        _embeddingsGenerationConfigurationByTaskIdentifiers = embeddingsGenerationConfigurationsByTaskIdentifier;

        if (Embeddings.QueryEmbeddingsCacher.IsRunning)
        {
            if (numberOfEmbeddingGenerationTasks == 0)
                Embeddings.QueryEmbeddingsCacher.Stop();
        }
        else
        {
            if (numberOfEmbeddingGenerationTasks > 0)
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
