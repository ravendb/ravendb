using System;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private readonly Dictionary<EmbeddingsGenerationTaskIdentifier, ITextEmbeddingGenerationService> _embeddingGeneratorsByTaskIdentifier;
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _embeddingGeneratorsByConnectionStringIdentifier;

    private Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTasks;
    private Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingGeneratorsConfigurationByTasks;

    public AiIntegrationsController(DocumentDatabase database)
    {
        _embeddingGeneratorsByConnectionStringIdentifier = new();
        _embeddingGeneratorsByTaskIdentifier = new();
        _embeddingGeneratorsConfigurationByTasks = new();
        _connectionStringsByTasks = new Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier>();

        var storage = new EmbeddingsStorage(database);
        var cacher = new EmbeddingsCacher(database, database.Loggers.GetLogger<EmbeddingsCacher>(), database.DatabaseShutdown);

        Embeddings = new EmbeddingsController(this, storage, cacher);
    }

    public EmbeddingsController Embeddings { get; private set; }

    public bool TryGetEmbeddingsGenerationConfiguration(EmbeddingsGenerationTaskIdentifier taskIdentifier, out EmbeddingsGenerationConfiguration configuration)
    {
        return _embeddingGeneratorsConfigurationByTasks.TryGetValue(taskIdentifier, out configuration);
    }

    public AiConnectionStringIdentifier GetConnectionStringByEmbeddingsGenerationTask(EmbeddingsGenerationTaskIdentifier taskIdentifier)
    {
        return _connectionStringsByTasks[taskIdentifier];
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

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

        // todo skip disabled tasks?
        foreach (var aiIntegrationConfiguration in record.EmbeddingsGenerations)
        {
            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(aiIntegrationConfiguration.Identifier);
            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[aiIntegrationConfiguration.ConnectionStringName].Identifier);

            var service = _embeddingGeneratorsByConnectionStringIdentifier[connectionStringIdentifier];

            _embeddingGeneratorsByTaskIdentifier[aiIntegrationIdentifier] = service;

            _connectionStringsByTasks[aiIntegrationIdentifier] = connectionStringIdentifier;

            _embeddingGeneratorsConfigurationByTasks[aiIntegrationIdentifier] = aiIntegrationConfiguration;
        }


        //if (_embeddingsCacher.IsStarted)
        //{
        //    if (record.AiIntegrations.Count == 0)
        //    {
        //        _embeddingsCacher.Stop();
        //        _embeddingsCacher.IsStarted = false;
        //    }

        //    return;
        //}


        //_embeddingsCacher.Start();
        //_embeddingsCacher.IsStarted = true;
    }

    public void Dispose()
    {
        //TODO arek
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringIdentifier, out ITextEmbeddingGenerationService service)
    {
        return _embeddingGeneratorsByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out service);
    }
}
