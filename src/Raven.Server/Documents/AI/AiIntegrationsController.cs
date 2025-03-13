using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Util;
using Raven.Server.Documents.AI.Embeddings;
using Sparrow.Server.Logging;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private readonly Dictionary<AiConnectionStringIdentifier, (AiConnectionString, ITextEmbeddingGenerationService)> _embeddingsGenerationServiceByConnectionStringIdentifier;

    private Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTaskIdentifiers;
    private Dictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingsGenerationConfigurationByTaskIdentifiers;

    private readonly RavenLogger _logger;
    private readonly SemaphoreSlim _databaseRecordChangeLock = new(initialCount: 1, maxCount: 1);

    public DocumentDatabase Database { get; }

    public AiIntegrationsController(DocumentDatabase database)
    {
        Database = database;
        _embeddingsGenerationServiceByConnectionStringIdentifier = new();
        _embeddingsGenerationConfigurationByTaskIdentifiers = new();
        _connectionStringsByTaskIdentifiers = new();
        _logger = database.Loggers.GetLogger<AiIntegrationsController>();

        var storage = new EmbeddingsStorage(database);
        var cacher = new QueryEmbeddingsCacher(database, database.DatabaseShutdown);

        Embeddings = new EmbeddingsController(this, storage, cacher);
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

    public async Task HandleDatabaseRecordChangeAsync(DatabaseRecord record)
    {
        if (record == null)
            return;

        await _databaseRecordChangeLock.WaitAsync();
        try
        {
            var activeConnectionStringsIds = UpdateEmbeddingsGenerationConfigurationsAndConnectionStrings(record);
            await UpdateEmbeddingGenerationServices(record, activeConnectionStringsIds);
            UpdateQueryEmbeddingsCacherState(activeConnectionStringsIds);

            if (_logger.IsInfoEnabled)
                _logger.Info($"Updated Embeddings Generation configurations, cacher state, and services for {activeConnectionStringsIds.Count} active connection strings.");
        }
        finally
        {
            _databaseRecordChangeLock.Release();
        }
    }

    private HashSet<AiConnectionStringIdentifier> UpdateEmbeddingsGenerationConfigurationsAndConnectionStrings(DatabaseRecord record)
    {
        var activeConnectionStrings = new HashSet<AiConnectionStringIdentifier>();
        var tasksToRetain = new HashSet<EmbeddingsGenerationTaskIdentifier>();

        foreach (var embeddingsGenerationConfiguration in record.EmbeddingsGenerations.Where(configuration => configuration.Disabled == false))
        {
            var taskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationConfiguration.Identifier);
            tasksToRetain.Add(taskId);

            // Updating task configuration
            _embeddingsGenerationConfigurationByTaskIdentifiers[taskId] = embeddingsGenerationConfiguration;

            // Updating connection string
            if (record.AiConnectionStrings.TryGetValue(embeddingsGenerationConfiguration.ConnectionStringName, out var connString) == false)
                continue;

            var activeConnectionStringId = new AiConnectionStringIdentifier(connString.Identifier);

            _connectionStringsByTaskIdentifiers[taskId] = activeConnectionStringId;
            activeConnectionStrings.Add(activeConnectionStringId);
        }

        // Cleaning up task configurations and connection strings which are not in the new DatabaseRecord
        foreach (var taskId in _embeddingsGenerationConfigurationByTaskIdentifiers.Keys.Except(tasksToRetain).ToList())
        {
            _embeddingsGenerationConfigurationByTaskIdentifiers.Remove(taskId);
            _connectionStringsByTaskIdentifiers.Remove(taskId);
        }

        return activeConnectionStrings;
    }

    private async Task UpdateEmbeddingGenerationServices(DatabaseRecord record, HashSet<AiConnectionStringIdentifier> activeConnectionStringIds)
    {
        foreach ((_, AiConnectionString newConnectionString) in record.AiConnectionStrings)
        {
            var connectionStringIdentifier = activeConnectionStringIds.FirstOrDefault(id => id.Value == newConnectionString.Identifier);
            if (connectionStringIdentifier == default)
                continue;

            _embeddingsGenerationServiceByConnectionStringIdentifier[connectionStringIdentifier] = (newConnectionString, AiHelper.CreateService(newConnectionString));
            await Embeddings.UpdateBatchingWorkerForConnectionStringIdAsync(newConnectionString);
        }

        // Remove services for connection strings that are not in the new DatabaseRecord
        foreach (var connectionStringId in _embeddingsGenerationServiceByConnectionStringIdentifier.Keys.Except(activeConnectionStringIds).ToList())
        {
            _embeddingsGenerationServiceByConnectionStringIdentifier.Remove(connectionStringId);
            await Embeddings.RemoveBatchingWorkerForConnectionStringIdAsync(connectionStringId);
        }
    }

    private void UpdateQueryEmbeddingsCacherState(HashSet<AiConnectionStringIdentifier> activeConnectionStringIds)
    {
        var hasActiveConnectionStrings = activeConnectionStringIds.Count > 0;

        if (Embeddings.QueryEmbeddingsCacher.IsRunning)
        {
            if (hasActiveConnectionStrings == false)
                Embeddings.QueryEmbeddingsCacher.Stop();
        }
        else if (hasActiveConnectionStrings)
        {
            Embeddings.QueryEmbeddingsCacher.Start();
        }
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringIdentifier, out (AiConnectionString ConnectionString, ITextEmbeddingGenerationService Instance) service)
    {
        return _embeddingsGenerationServiceByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out service);
    }

    public void Initialize(DatabaseRecord record)
    {
        AsyncHelpers.RunSync(() => HandleDatabaseRecordChangeAsync(record));
    }

    private async Task DisposeAsync()
    {
        if (Embeddings != null)
            await Embeddings.DisposeAsync();

        _embeddingsGenerationServiceByConnectionStringIdentifier.Clear();
        _databaseRecordChangeLock.Dispose();
    }

    public void Dispose() => AsyncHelpers.RunSync(DisposeAsync);
}
