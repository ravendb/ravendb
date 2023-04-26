using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexCreateController
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexCreateController([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract string GetDatabaseName();

    protected abstract SystemTime GetDatabaseTime();

    public abstract RavenConfiguration GetDatabaseConfiguration();

    protected abstract IndexInformationHolder GetIndex(string name);

    protected abstract IEnumerable<string> GetIndexNames();

    protected abstract ValueTask<long> GetCollectionCountAsync(string collection);

    protected abstract IEnumerable<IndexInformationHolder> GetIndexes();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index, TimeSpan? timeout = null);

    protected virtual async ValueTask ValidateStaticIndexAsync(IndexDefinition definition)
    {
        if (IndexStore.IsValidIndexName(definition.Name, true, out var errorMessage) == false)
        {
            throw new ArgumentException(errorMessage);
        }

        ServerStore.LicenseManager.AssertCanAddAdditionalAssembliesFromNuGet(definition);

        var safeFileSystemIndexName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(definition.Name);

        var indexWithFileSystemNameCollision = GetIndexNames().FirstOrDefault(x =>
            x.Equals(definition.Name, StringComparison.OrdinalIgnoreCase) == false &&
            safeFileSystemIndexName.Equals(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(x), StringComparison.OrdinalIgnoreCase));

        if (indexWithFileSystemNameCollision != null)
            throw new IndexCreationException(
                $"Could not create index '{definition.Name}' because it would result in directory name collision with '{indexWithFileSystemNameCollision}' index");

        definition.RemoveDefaultValues();
        ValidateAnalyzers(definition);

        var databaseConfiguration = GetDatabaseConfiguration();
        var instance = IndexCompilationCache.GetIndexInstance(definition, databaseConfiguration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion); // pre-compile it and validate

        if (definition.Type == IndexType.MapReduce)
        {
            await MapReduceIndex.ValidateReduceResultsCollectionNameAsync(
                definition,
                instance,
                GetIndexes,
                GetCollectionCountAsync,
                NeedToCheckIfCollectionEmpty(definition, databaseConfiguration));

            if (string.IsNullOrEmpty(definition.PatternForOutputReduceToCollectionReferences) == false)
                OutputReferencesPattern.ValidatePattern(definition.PatternForOutputReduceToCollectionReferences, out _);
        }
    }

    protected virtual void ValidateAutoIndex(IndexDefinitionBaseServerSide definition)
    {
        if (IndexStore.IsValidIndexName(definition.Name, false, out var errorMessage) == false)
        {
            throw new ArgumentException(errorMessage);
        }
    }

    public async ValueTask<long> CreateIndexAsync(IndexDefinition definition, string raftRequestId, string source = null)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        await ValidateStaticIndexAsync(definition);

        var databaseName = GetDatabaseName();
        var databaseTime = GetDatabaseTime();
        var databaseConfiguration = GetDatabaseConfiguration();

        var command = new PutIndexCommand(
            definition,
            databaseName,
            source,
            databaseTime.GetUtcNow(),
            raftRequestId,
            databaseConfiguration.Indexing.HistoryRevisionsNumber,
            databaseConfiguration.Indexing.StaticIndexDeploymentMode
        );

        long index = 0;
        try
        {
            index = (await ServerStore.SendToLeaderAsync(command)).Index;
        }
        catch (Exception e)
        {
            IndexStore.ThrowIndexCreationException("static", definition.Name, e, "the cluster is probably down", ServerStore);
        }

        try
        {
            await WaitForIndexNotificationAsync(index);
        }
        catch (TimeoutException toe)
        {
            IndexStore.ThrowIndexCreationException("static", definition.Name, toe, $"the operation timed out after: {ServerStore.Engine.OperationTimeout}.", ServerStore);
        }

        return index;
    }

    public async ValueTask<long?> CreateIndexAsync(IndexDefinitionBaseServerSide definition, string raftRequestId)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        if (definition is MapIndexDefinition mapIndexDefinition)
            return await CreateIndexAsync(mapIndexDefinition.IndexDefinition, raftRequestId);

        var configuration = GetDatabaseConfiguration();

        definition.DeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;

        ValidateAutoIndex(definition);

        var command = PutAutoIndexCommand.Create((AutoIndexDefinitionBaseServerSide)definition, GetDatabaseName(), raftRequestId, configuration.Indexing.AutoIndexDeploymentMode);

        long index = 0;
        try
        {
            index = (await ServerStore.SendToLeaderAsync(command).ConfigureAwait(false)).Index;
        }
        catch (Exception e)
        {
            IndexStore.ThrowIndexCreationException("auto", definition.Name, e, "the cluster is probably down", ServerStore);
        }

        try
        {
            await WaitForIndexNotificationAsync(index);
        }
        catch (TimeoutException toe)
        {
            IndexStore.ThrowIndexCreationException("auto", definition.Name, toe, $"the operation timed out after: {ServerStore.Engine.OperationTimeout}.", ServerStore);
        }

        return index;
    }

    public static bool CanUseIndexBatch()
    {
        return ClusterCommandsVersionManager.CanPutCommand(nameof(PutIndexesCommand));
    }

    public IndexBatchScope CreateIndexBatch()
    {
        return new IndexBatchScope(this, ServerStore, ServerStore.LicenseManager.GetNumberOfUtilizedCores());
    }

    private void ValidateAnalyzers(IndexDefinition definition)
    {
        if (definition.Fields == null)
            return;

        foreach (var kvp in definition.Fields)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value.Analyzer))
                continue;

            try
            {
                LuceneIndexingExtensions.GetAnalyzerType(kvp.Key, kvp.Value.Analyzer, GetDatabaseName());
            }
            catch (Exception e)
            {
                throw new IndexCompilationException(e.Message, e);
            }
        }
    }

    private bool NeedToCheckIfCollectionEmpty(IndexDefinition definition, RavenConfiguration databaseConfiguration)
    {
        var currentIndex = GetIndex(definition.Name);
        var replacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + definition.Name;
        var replacementIndex = GetIndex(replacementIndexName);
        if (currentIndex == null && replacementIndex == null)
        {
            // new index
            return true;
        }

        if (currentIndex == null)
        {
            // we deleted the in memory index but didn't delete the replacement yet
            return true;
        }

        var creationOptions = IndexStore.GetIndexCreationOptions(definition, currentIndex, databaseConfiguration, out var _);
        IndexCreationOptions replacementCreationOptions;
        if (replacementIndex != null)
        {
            replacementCreationOptions = IndexStore.GetIndexCreationOptions(definition, replacementIndex, databaseConfiguration, out var _);
        }
        else
        {
            // the replacement index doesn't exist
            return IsCreateOrUpdate(creationOptions);
        }

        return IsCreateOrUpdate(creationOptions) ||
               IsCreateOrUpdate(replacementCreationOptions);
    }

    private static bool IsCreateOrUpdate(IndexCreationOptions creationOptions)
    {
        return creationOptions == IndexCreationOptions.Create ||
               creationOptions == IndexCreationOptions.Update;
    }

    public class IndexBatchScope
    {
        private readonly AbstractIndexCreateController _controller;
        private readonly ServerStore _serverStore;
        private readonly int _numberOfUtilizedCores;

        private PutIndexesCommand _command;
        private readonly IndexDeploymentMode _defaultAutoDeploymentMode;
        private readonly IndexDeploymentMode _defaultStaticDeploymentMode;

        public IndexBatchScope([NotNull] AbstractIndexCreateController controller, [NotNull] ServerStore serverStore, int numberOfUtilizedCores)
        {
            _controller = controller ?? throw new ArgumentNullException(nameof(controller));
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _numberOfUtilizedCores = numberOfUtilizedCores;

            var configuration = controller.GetDatabaseConfiguration();

            _defaultAutoDeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;
            _defaultStaticDeploymentMode = configuration.Indexing.StaticIndexDeploymentMode;
        }

        public async ValueTask AddIndexAsync(IndexDefinitionBaseServerSide definition, string source, DateTime createdAt, string raftRequestId, int revisionsToKeep)
        {
            if (_command == null)
                _command = new PutIndexesCommand(_controller.GetDatabaseName(), source, createdAt, raftRequestId, revisionsToKeep, _defaultAutoDeploymentMode, _defaultStaticDeploymentMode);

            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition indexDefinition)
            {
                await AddIndexAsync(indexDefinition.IndexDefinition, source, createdAt, raftRequestId, revisionsToKeep);
                return;
            }

            _controller.ValidateAutoIndex(definition);

            var autoDefinition = (AutoIndexDefinitionBaseServerSide)definition;
            var indexType = PutAutoIndexCommand.GetAutoIndexType(autoDefinition);

            _command.Auto.Add(PutAutoIndexCommand.GetAutoIndexDefinition(autoDefinition, indexType));
        }

        public async ValueTask AddIndexAsync(IndexDefinition definition, string source, DateTime createdAt, string raftRequestId, int revisionsToKeep)
        {
            if (_command == null)
                _command = new PutIndexesCommand(_controller.GetDatabaseName(), source, createdAt, raftRequestId, revisionsToKeep, _defaultAutoDeploymentMode, _defaultStaticDeploymentMode);

            await _controller.ValidateStaticIndexAsync(definition);

            _command.Static.Add(definition);
        }

        public async Task SaveIfNeeded()
        {
            if (_command == null)
                return;

            if (_command.Static.Count + _command.Auto.Count > 50)
            {
                await SaveAsync();
            }
        }

        public async Task SaveAsync()
        {
            if (_command == null || _command.Static.Count == 0 && _command.Auto.Count == 0)
                return;

            try
            {
                long index = 0;
                try
                {
                    index = (await _serverStore.SendToLeaderAsync(_command)).Index;
                }
                catch (Exception e)
                {
                    ThrowIndexCreationException(e, "Cluster is probably down.");
                }

                var indexCount = _command.Static.Count + _command.Auto.Count;
                var operationTimeout = _serverStore.Engine.OperationTimeout;
                var timeout = TimeSpan.FromSeconds(((double)indexCount / _numberOfUtilizedCores) * operationTimeout.TotalSeconds);
                if (operationTimeout > timeout)
                    timeout = operationTimeout;

                try
                {
                    await _controller.WaitForIndexNotificationAsync(index, timeout);
                }
                catch (TimeoutException toe)
                {
                    ThrowIndexCreationException(toe, $"Operation timed out after: {timeout}.");
                }
            }
            finally
            {
                _command = null;
            }
        }

        private void ThrowIndexCreationException(Exception exception, string reason)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Failed to create indexes. {reason}");
            if (_command.Static != null && _command.Static.Count > 0)
                sb.AppendLine("Static: " + string.Join(", ", _command.Static.Select(x => x.Name)));

            if (_command.Auto != null && _command.Auto.Count > 0)
                sb.AppendLine("Auto: " + string.Join(", ", _command.Auto.Select(x => x.Name)));

            sb.AppendLine($"Node {_serverStore.NodeTag} state is {_serverStore.LastStateChangeReason()}");

            throw new IndexCreationException(sb.ToString(), exception);
        }
    }
}
