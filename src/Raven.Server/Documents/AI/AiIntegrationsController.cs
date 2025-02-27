using System;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _embeddingGeneratorsByConnectionStringIdentifier;

    private Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTaskIdentifiers;
    private Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingGeneratorsConfigurationByTaskIdentifers;

    public AiIntegrationsController(DocumentDatabase database)
    {
        _embeddingGeneratorsByConnectionStringIdentifier = new();
        _embeddingGeneratorsConfigurationByTaskIdentifers = new();
        _connectionStringsByTaskIdentifiers = new();

        var storage = new EmbeddingsStorage(database);
        var cacher = new EmbeddingsCacher(database, database.DatabaseShutdown);

        Embeddings = new EmbeddingsController(this, storage, cacher);
    }

    public EmbeddingsController Embeddings { get; private set; }

    public bool TryGetEmbeddingsGenerationConfiguration(EmbeddingsGenerationTaskIdentifier taskIdentifier, out EmbeddingsGenerationConfiguration configuration)
    {
        return _embeddingGeneratorsConfigurationByTaskIdentifers.TryGetValue(taskIdentifier, out configuration);
    }

    public AiConnectionStringIdentifier GetConnectionStringByEmbeddingsGenerationTask(EmbeddingsGenerationTaskIdentifier taskIdentifier)
    {
        return _connectionStringsByTaskIdentifiers[taskIdentifier];
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

        var connectionStringsByTasks = new Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier>();
        var embeddingGeneratorsConfigurationByTasks = new Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration>();

        foreach (var connectionStringKvp in record.AiConnectionStrings)
        {
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionStringKvp.Value.Identifier);
            var connectionString = connectionStringKvp.Value;

            if (_embeddingGeneratorsByConnectionStringIdentifier.ContainsKey(connectionStringIdentifier))
                continue;

            var kernelBuilder = Kernel.CreateBuilder();
            kernelBuilder.Configure(connectionString, isConnectionTest: false);
            var kernel = kernelBuilder.Build();
            var service = kernel.GetRequiredService<ITextEmbeddingGenerationService>();

            _embeddingGeneratorsByConnectionStringIdentifier[connectionStringIdentifier] = service;
        }

        var numberOfActiveEmbeddingGenerationTasks = 0;

        foreach (var embeddingGenerationConfiguration in record.EmbeddingsGenerations)
        {
            if (embeddingGenerationConfiguration.Disabled == false)
                numberOfActiveEmbeddingGenerationTasks++;

            var embeddingsGeneratorIdentifier = new EmbeddingsGenerationTaskIdentifier(embeddingGenerationConfiguration.Identifier);
            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[embeddingGenerationConfiguration.ConnectionStringName].Identifier);

            connectionStringsByTasks[embeddingsGeneratorIdentifier] = connectionStringIdentifier;

            embeddingGeneratorsConfigurationByTasks[embeddingsGeneratorIdentifier] = embeddingGenerationConfiguration;
        }

        _connectionStringsByTaskIdentifiers = connectionStringsByTasks;
        _embeddingGeneratorsConfigurationByTaskIdentifers = embeddingGeneratorsConfigurationByTasks;

        if (Embeddings.Cacher.IsRunning)
        {
            if (numberOfActiveEmbeddingGenerationTasks == 0)
                Embeddings.Cacher.Stop();
        }
        else
        {
            Embeddings.Cacher.Start();
        }
    }

    public void Dispose()
    {
        Embeddings.Cacher.Dispose();
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringIdentifier, out ITextEmbeddingGenerationService service)
    {
        return _embeddingGeneratorsByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out service);
    }
}
