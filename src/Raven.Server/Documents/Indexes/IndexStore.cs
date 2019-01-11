using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.IndexMerging;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _documentDatabase;
        private readonly ServerStore _serverStore;

        private readonly CollectionOfIndexes _indexes = new CollectionOfIndexes();
        private readonly ConcurrentDictionary<string, object> _indexLocks = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        private bool _initialized;

        private bool _run = true;

        public readonly IndexIdentities Identities = new IndexIdentities();

        public Logger Logger => _logger;

        public SemaphoreSlim StoppedConcurrentIndexBatches { get; }

        internal Action<(string IndexName, bool DidWork)> IndexBatchCompleted;

        public IndexStore(DocumentDatabase documentDatabase, ServerStore serverStore)
        {
            _documentDatabase = documentDatabase;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<IndexStore>(_documentDatabase.Name);

            var stoppedConcurrentIndexBatches = _documentDatabase.Configuration.Indexing.NumberOfConcurrentStoppedBatchesIfRunningLowOnMemory;
            StoppedConcurrentIndexBatches = new SemaphoreSlim(stoppedConcurrentIndexBatches);
        }

        public void HandleDatabaseRecordChange(DatabaseRecord record, long index)
        {
            if (record == null)
                return;

            HandleDeletes(record, index);
            HandleChangesForStaticIndexes(record, index);
            HandleChangesForAutoIndexes(record, index);
        }

        private void HandleChangesForAutoIndexes(DatabaseRecord record, long index)
        {
            foreach (var kvp in record.AutoIndexes)
            {
                _documentDatabase.DatabaseShutdown.ThrowIfCancellationRequested();

                var name = kvp.Key;
                try
                {
                    var definition = CreateAutoDefinition(kvp.Value);

                    HandleAutoIndexChange(name, definition);
                }
                catch (Exception e)
                {
                    _documentDatabase.RachisLogIndexNotifications.NotifyListenersAbout(index, e);
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not create auto index {name}", e);
                }
            }
        }

        private void HandleAutoIndexChange(string name, AutoIndexDefinitionBase definition)
        {
            lock (GetIndexLock(name))
            {
                var creationOptions = IndexCreationOptions.Create;
                var existingIndex = GetIndex(name);
                IndexDefinitionCompareDifferences differences = IndexDefinitionCompareDifferences.None;
                if (existingIndex != null)
                    creationOptions = GetIndexCreationOptions(definition, existingIndex, out differences);

                if (creationOptions == IndexCreationOptions.Noop)
                {
                    Debug.Assert(existingIndex != null);

                    return;
                }

                if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex || creationOptions == IndexCreationOptions.Update)
                {
                    Debug.Assert(existingIndex != null);

                    if ((differences & IndexDefinitionCompareDifferences.LockMode) != 0)
                    {
                        existingIndex.SetLock(definition.LockMode);
                    }

                    if ((differences & IndexDefinitionCompareDifferences.Priority) != 0)
                    {
                        existingIndex.SetPriority(definition.Priority);
                    }

                    existingIndex.Update(definition, existingIndex.Configuration);

                    return;
                }

                Index index;

                if (definition is AutoMapIndexDefinition)
                    index = AutoMapIndex.CreateNew((AutoMapIndexDefinition)definition, _documentDatabase);
                else if (definition is AutoMapReduceIndexDefinition)
                    index = AutoMapReduceIndex.CreateNew((AutoMapReduceIndexDefinition)definition, _documentDatabase);
                else
                    throw new NotImplementedException($"Unknown index definition type: {definition.GetType().FullName}");

                CreateIndexInternal(index);
            }
        }

        internal static AutoIndexDefinitionBase CreateAutoDefinition(AutoIndexDefinition definition)
        {
            var mapFields = definition
                .MapFields
                .Select(x =>
                {
                    var field = AutoIndexField.Create(x.Key, x.Value);

                    Debug.Assert(x.Value.GroupByArrayBehavior == GroupByArrayBehavior.NotApplicable);

                    return field;
                })
                .ToArray();

            if (definition.Type == IndexType.AutoMap)
            {
                var result = new AutoMapIndexDefinition(definition.Collection, mapFields);

                if (definition.Priority.HasValue)
                    result.Priority = definition.Priority.Value;

                return result;
            }

            if (definition.Type == IndexType.AutoMapReduce)
            {
                var groupByFields = definition
                    .GroupByFields
                    .Select(x =>
                    {
                        var field = AutoIndexField.Create(x.Key, x.Value);

                        return field;
                    })
                    .ToArray();

                var result = new AutoMapReduceIndexDefinition(definition.Collection, mapFields, groupByFields);

                if (definition.Priority.HasValue)
                    result.Priority = definition.Priority.Value;

                return result;
            }

            throw new NotSupportedException("Cannot create auto-index from " + definition.Type);
        }

        private void HandleChangesForStaticIndexes(DatabaseRecord record, long index)
        {
            foreach (var kvp in record.Indexes)
            {
                _documentDatabase.DatabaseShutdown.ThrowIfCancellationRequested();

                var name = kvp.Key;
                var definition = kvp.Value;

                try
                {
                    HandleStaticIndexChange(name, definition);
                }
                catch (Exception exception)
                {
                    _documentDatabase.RachisLogIndexNotifications.NotifyListenersAbout(index, exception);

                    var indexName = name;
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not update static index {name}", exception);
                    //If we don't have the index in memory this means that it is corrupted when trying to load it
                    //If we do have the index and it is not faulted this means that this is the replacment index that is faulty
                    //If we already have a replacment that is faulty don't add a new one
                    if (_indexes.TryGetByName(indexName, out Index i))
                    {
                        if (i is FaultyInMemoryIndex)
                            return;
                        indexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + name;
                        if (_indexes.TryGetByName(indexName, out Index j) && j is FaultyInMemoryIndex)
                            return;
                    }

                    var configuration = new FaultyInMemoryIndexConfiguration(_documentDatabase.Configuration.Indexing.StoragePath, _documentDatabase.Configuration);
                    var fakeIndex = new FaultyInMemoryIndex(exception, indexName, configuration, definition);
                    _indexes.Add(fakeIndex);
                }
            }
        }

        private void HandleStaticIndexChange(string name, IndexDefinition definition)
        {
            lock (GetIndexLock(name))
            {
                var creationOptions = IndexCreationOptions.Create;
                var currentIndex = GetIndex(name);
                IndexDefinitionCompareDifferences currentDifferences = IndexDefinitionCompareDifferences.None;

                if (currentIndex != null)
                    creationOptions = GetIndexCreationOptions(definition, currentIndex, out currentDifferences);

                var replacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + definition.Name;

                if (creationOptions == IndexCreationOptions.Noop)
                {
                    Debug.Assert(currentIndex != null);

                    var replacementIndex = GetIndex(replacementIndexName);
                    if (replacementIndex != null)
                        DeleteIndexInternal(replacementIndex);

                    return;
                }

                if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                {
                    Debug.Assert(currentIndex != null);

                    var replacementIndex = GetIndex(replacementIndexName);
                    if (replacementIndex != null)
                        DeleteIndexInternal(replacementIndex);

                    if (currentDifferences != IndexDefinitionCompareDifferences.None)
                        UpdateIndex(definition, currentIndex, currentDifferences);
                    return;
                }

                UpdateStaticIndexLockModeAndPriority(definition, currentIndex, currentDifferences);

                if (creationOptions == IndexCreationOptions.Update)
                {
                    Debug.Assert(currentIndex != null);

                    definition.Name = replacementIndexName;
                    var replacementIndex = GetIndex(replacementIndexName);
                    if (replacementIndex != null)
                    {
                        creationOptions = GetIndexCreationOptions(definition, replacementIndex, out IndexDefinitionCompareDifferences sideBySideDifferences);
                        if (creationOptions == IndexCreationOptions.Noop)
                            return;

                        if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                        {
                            UpdateIndex(definition, replacementIndex, sideBySideDifferences);
                            return;
                        }

                        DeleteIndexInternal(replacementIndex);
                    }
                }

                Index index;
                switch (definition.Type)
                {
                    case IndexType.Map:
                        index = MapIndex.CreateNew(definition, _documentDatabase);
                        break;
                    case IndexType.MapReduce:
                        index = MapReduceIndex.CreateNew(definition, _documentDatabase);
                        break;
                    default:
                        throw new NotSupportedException($"Cannot create {definition.Type} index from IndexDefinition");
                }

                CreateIndexInternal(index);
            }
        }

        private void HandleDeletes(DatabaseRecord record, long raftLogIndex)
        {
            foreach (var index in _indexes)
            {
                _documentDatabase.DatabaseShutdown.ThrowIfCancellationRequested();

                var indexNormalizedName = index.Name;
                if (indexNormalizedName.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix))
                {
                    indexNormalizedName = indexNormalizedName.Remove(0, Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length);
                }
                if (record.Indexes.ContainsKey(indexNormalizedName) || record.AutoIndexes.ContainsKey(indexNormalizedName))
                    continue;

                try
                {
                    DeleteIndexInternal(index);
                }
                catch (Exception e)
                {
                    _documentDatabase.RachisLogIndexNotifications.NotifyListenersAbout(raftLogIndex, e);
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not delete index {index.Name}", e);
                }
            }
        }

        public Task InitializeAsync(DatabaseRecord record)
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

            InitializePath(_documentDatabase.Configuration.Indexing.StoragePath);

            _initialized = true;

            return Task.Run(() => { OpenIndexes(record); });
        }

        public Index GetIndex(string name)
        {
            if (_indexes.TryGetByName(name, out Index index) == false)
                return null;

            return index;
        }

        public async Task<Index> CreateIndex(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            ValidateIndexName(definition.Name, isStatic: true);
            definition.RemoveDefaultValues();
            ValidateAnalyzers(definition);

            var instance = IndexCompilationCache.GetIndexInstance(definition); // pre-compile it and validate

            if (definition.Type == IndexType.MapReduce)
                MapReduceIndex.ValidateReduceResultsCollectionName(definition, instance, _documentDatabase, NeedToCheckIfCollectionEmpty(definition));

            var command = new PutIndexCommand(definition, _documentDatabase.Name);

            try
            {
                var (etag, _) = await _serverStore.SendToLeaderAsync(command);
                await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

                return GetIndex(definition.Name);
            }
            catch (TimeoutException toe)
            {
                throw new IndexCreationException($"Failed to create static index: {definition.Name}, the cluster is probably down. " +
                                                 $"Node {_serverStore.NodeTag} state is {_serverStore.LastStateChangeReason()}", toe);
            }
        }

        private bool NeedToCheckIfCollectionEmpty(IndexDefinition definition)
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

            var creationOptions = GetIndexCreationOptions(definition, currentIndex, out var _);
            IndexCreationOptions replacementCreationOptions;
            if (replacementIndex != null)
            {
                replacementCreationOptions = GetIndexCreationOptions(definition, replacementIndex, out var _);
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

        public async Task<Index> CreateIndex(IndexDefinitionBase definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition)
                return await CreateIndex(((MapIndexDefinition)definition).IndexDefinition);

            ValidateIndexName(definition.Name, isStatic: false);

            try
            {
                var command = PutAutoIndexCommand.Create((AutoIndexDefinitionBase)definition, _documentDatabase.Name);

                var (etag, _) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

                var instance = GetIndex(definition.Name);

                return instance;
            }
            catch (TimeoutException toe)
            {
                throw new IndexCreationException($"Failed to create auto index: {definition.Name}, the cluster is probably down. " +
                                                     $"Node {_serverStore.NodeTag} state is {_serverStore.LastStateChangeReason()}", toe);
            }
        }

        public bool HasChanged(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            ValidateIndexName(definition.Name, isStatic: true);

            var existingIndex = GetIndex(definition.Name);
            if (existingIndex == null)
                return true;

            var creationOptions = GetIndexCreationOptions(definition, existingIndex, out IndexDefinitionCompareDifferences _);
            return creationOptions != IndexCreationOptions.Noop;
        }

        private void CreateIndexInternal(Index index)
        {
            Debug.Assert(index != null);
            Debug.Assert(string.IsNullOrEmpty(index.Name) == false);

            _indexes.Add(index);

            if (_documentDatabase.Configuration.Indexing.Disabled == false && _run)
                index.Start();

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded
                });
        }

        private void UpdateIndex(IndexDefinition definition, Index existingIndex, IndexDefinitionCompareDifferences indexDifferences)
        {
            UpdateStaticIndexLockModeAndPriority(definition, existingIndex, indexDifferences);

            switch (definition.Type)
            {
                case IndexType.Map:
                    MapIndex.Update(existingIndex, definition, _documentDatabase);
                    break;
                case IndexType.MapReduce:
                    MapReduceIndex.Update(existingIndex, definition, _documentDatabase);
                    break;
                default:
                    throw new NotSupportedException($"Cannot update {definition.Type} index from IndexDefinition");
            }
        }

        private static void UpdateStaticIndexLockModeAndPriority(IndexDefinition definition, Index existingIndex, IndexDefinitionCompareDifferences indexDifferences)
        {
            if (definition.LockMode.HasValue && (indexDifferences & IndexDefinitionCompareDifferences.LockMode) != 0)
                existingIndex.SetLock(definition.LockMode.Value);

            if (definition.Priority.HasValue && (indexDifferences & IndexDefinitionCompareDifferences.Priority) != 0)
                existingIndex.SetPriority(definition.Priority.Value);
        }

        internal IndexCreationOptions GetIndexCreationOptions(object indexDefinition, Index existingIndex, out IndexDefinitionCompareDifferences differences)
        {
            differences = IndexDefinitionCompareDifferences.All;
            if (existingIndex == null)
                return IndexCreationOptions.Create;

            differences = IndexDefinitionCompareDifferences.None;

            var indexDef = indexDefinition as IndexDefinition;
            if (indexDef != null)
                differences = existingIndex.Definition.Compare(indexDef);

            var indexDefBase = indexDefinition as IndexDefinitionBase;
            if (indexDefBase != null)
                differences = existingIndex.Definition.Compare(indexDefBase);

            if (differences == IndexDefinitionCompareDifferences.All)
                return IndexCreationOptions.Update;

            if (differences == IndexDefinitionCompareDifferences.None)
                return IndexCreationOptions.Noop;

            if ((differences & IndexDefinitionCompareDifferences.Maps) == IndexDefinitionCompareDifferences.Maps ||
                (differences & IndexDefinitionCompareDifferences.Reduce) == IndexDefinitionCompareDifferences.Reduce)
                return IndexCreationOptions.Update;

            if ((differences & IndexDefinitionCompareDifferences.Fields) == IndexDefinitionCompareDifferences.Fields)
                return IndexCreationOptions.Update;

            if ((differences & IndexDefinitionCompareDifferences.Configuration) == IndexDefinitionCompareDifferences.Configuration)
            {
                var currentConfiguration = existingIndex.Configuration as SingleIndexConfiguration;
                if (currentConfiguration == null) // should not happen
                    return IndexCreationOptions.Update;

                var newConfiguration = new SingleIndexConfiguration(indexDef.Configuration, _documentDatabase.Configuration);
                var configurationResult = currentConfiguration.CalculateUpdateType(newConfiguration);
                switch (configurationResult)
                {
                    case IndexUpdateType.None:
                        break;
                    case IndexUpdateType.Refresh:
                        return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;
                    case IndexUpdateType.Reset:
                        return IndexCreationOptions.Update;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if ((differences & IndexDefinitionCompareDifferences.MapsFormatting) == IndexDefinitionCompareDifferences.MapsFormatting ||
                (differences & IndexDefinitionCompareDifferences.ReduceFormatting) == IndexDefinitionCompareDifferences.ReduceFormatting)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((differences & IndexDefinitionCompareDifferences.Priority) == IndexDefinitionCompareDifferences.Priority)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((differences & IndexDefinitionCompareDifferences.LockMode) == IndexDefinitionCompareDifferences.LockMode)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            return IndexCreationOptions.Update;
        }

        public static bool IsValidIndexName(string name, bool isStatic, out string errorMessage)
        {
            errorMessage = null;

            try
            {
                ValidateIndexName(name, isStatic);
            }
            catch (Exception e)
            {
                errorMessage = e.Message.Split('\n')[0];
                return false;
            }

            return true;
        }

        public static void ValidateIndexName(string name, bool isStatic)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Index name cannot be empty!");

            if (name.Contains("//"))
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index name cannot contain // (double slashes)", nameof(name));

            if (isStatic && name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException($"Index name '{name}' not permitted. Static index name cannot start with 'Auto/'", nameof(name));

            if (isStatic && NameUtils.IsValidIndexName(name) == false)
                throw new ArgumentException($"Index name '{name}' is not permitted. Only letters, digits and characters that match regex '{NameUtils.ValidIndexNameCharacters}' are allowed.", nameof(name));
        }

        public Index ResetIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            return ResetIndexInternal(index);
        }

        public async Task<bool> TryDeleteIndexIfExists(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                return false;

            if (name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix))
            {
                await HandleSideBySideIndexDelete(name);
                return true;
            }

            var (etag, _) = await _serverStore.SendToLeaderAsync(new DeleteIndexCommand(index.Name, _documentDatabase.Name));

            await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

            return true;
        }

        private async Task HandleSideBySideIndexDelete(string name)
        {
            var originalIndexName = name.Remove(0, Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length);
            var originalIndex = GetIndex(originalIndexName);
            if (originalIndex == null)
            {
                // we cannot find the original index 
                // but we need to remove the side by side one by the original name
                var (etag, _) = await _serverStore.SendToLeaderAsync(new DeleteIndexCommand(originalIndexName, _documentDatabase.Name));

                await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

                return;
            }

            // deleting the side by side index means that we need to save the original one in the database record

            var indexDefinition = originalIndex.GetIndexDefinition();
            indexDefinition.Name = originalIndexName;
            await CreateIndex(indexDefinition);
        }

        public async Task DeleteIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            var (newEtag, _) = await _serverStore.SendToLeaderAsync(new DeleteIndexCommand(index.Name, _documentDatabase.Name));

            await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(newEtag, _serverStore.Engine.OperationTimeout);
        }

        private void DeleteIndexInternal(Index index, bool raiseNotification = true)
        {
            _indexes.TryRemoveByName(index.Name, index);

            try
            {
                index.Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Could not dispose index '{index.Name}'.", e);
            }

            if (raiseNotification)
            {
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved
                });
            }

            if (index.Configuration.RunInMemory)
                return;

            var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name);

            var indexPath = index.Configuration.StoragePath.Combine(name);

            var indexTempPath = index.Configuration.TempPath?.Combine(name);

            try
            {
                IOExtensions.DeleteDirectory(indexPath.FullPath);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to delete the index {name} directory", e);
                throw;
            }

            if (indexTempPath != null)
                IOExtensions.DeleteDirectory(indexTempPath.FullPath);
        }

        public IndexRunningStatus Status
        {
            get
            {
                if (_documentDatabase.Configuration.Indexing.Disabled)
                    return IndexRunningStatus.Disabled;

                if (_run)
                    return IndexRunningStatus.Running;

                return IndexRunningStatus.Paused;
            }
        }

        public long Count => _indexes.Count;

        public void StartIndexing()
        {
            _run = true;

            StartIndexing(_indexes);
        }

        public void StartMapIndexes()
        {
            StartIndexing(_indexes.Where(x => x.Type.IsMap()));
        }

        public void StartMapReduceIndexes()
        {
            StartIndexing(_indexes.Where(x => x.Type.IsMapReduce()));
        }

        private void StartIndexing(IEnumerable<Index> indexes)
        {
            if (_documentDatabase.Configuration.Indexing.Disabled)
                return;

            Parallel.ForEach(indexes, index => index.Start());
        }

        public void StartIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Start();
        }

        public void StopIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Stop(disableIndex: true);

            _documentDatabase.Changes.RaiseNotifications(new IndexChange
            {
                Name = name,
                Type = IndexChangeTypes.IndexPaused
            });
        }

        public void StopIndexing()
        {
            _run = false;

            StopIndexing(_indexes);
        }

        public void StopMapIndexes()
        {
            StopIndexing(_indexes.Where(x => x.Type.IsMap()));
        }

        public void StopMapReduceIndexes()
        {
            StopIndexing(_indexes.Where(x => x.Type.IsMapReduce()));
        }

        private void StopIndexing(IEnumerable<Index> indexes)
        {
            if (_documentDatabase.Configuration.Indexing.Disabled)
                return;

            var list = indexes.ToList();
            Parallel.ForEach(list, index =>
            {
                try
                {
                    index.Stop(disableIndex: true);
                }
                catch (ObjectDisposedException)
                {
                    // index was deleted ?
                }
            });

            foreach (var index in list)
            {
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexPaused
                });
            }
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(IndexStore)}");

            Parallel.ForEach(_indexes, index =>
            {
                if (index is FaultyInMemoryIndex)
                    return;

                exceptionAggregator.Execute(index.Dispose);
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        private Index ResetIndexInternal(Index index)
        {
            try
            {
                DeleteIndexInternal(index);
            }
            catch (Exception toe)
            {
                throw new IndexDeletionException($"Failed to reset index: {index.Name}.", toe);
            }

            try
            {
                var definitionBase = index.Definition;
                if (definitionBase is FaultyAutoIndexDefinition faultyAutoIndexDefinition)
                    definitionBase = faultyAutoIndexDefinition.Definition;

                if (definitionBase is AutoMapIndexDefinition)
                    index = AutoMapIndex.CreateNew((AutoMapIndexDefinition)definitionBase, _documentDatabase);
                else if (definitionBase is AutoMapReduceIndexDefinition)
                    index = AutoMapReduceIndex.CreateNew((AutoMapReduceIndexDefinition)definitionBase, _documentDatabase);
                else
                {
                    var staticIndexDefinition = index.Definition.GetOrCreateIndexDefinitionInternal();
                    switch (staticIndexDefinition.Type)
                    {
                        case IndexType.Map:
                            index = MapIndex.CreateNew(staticIndexDefinition, _documentDatabase);
                            break;
                        case IndexType.MapReduce:
                            index = MapReduceIndex.CreateNew(staticIndexDefinition, _documentDatabase, isIndexReset: true);
                            break;
                        default:
                            throw new NotSupportedException($"Cannot create {staticIndexDefinition.Type} index from IndexDefinition");
                    }
                }

                CreateIndexInternal(index);

                return index;
            }
            catch (TimeoutException toe)
            {
                throw new IndexCreationException($"Failed to reset index: {index.Name}", toe);
            }
            catch (Exception e)
            {
                throw new IndexCreationException($"Failed to reset index: {index.Name}", e);
            }
        }

        private void OpenIndexes(DatabaseRecord record)
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            // apply renames
            OpenIndexesFromRecord(_documentDatabase.Configuration.Indexing.StoragePath, record);
        }

        private void OpenIndexesFromRecord(PathSetting path, DatabaseRecord record)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to load indexes from record");

            List<Exception> exceptions = null;
            if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexCannotBeOpened)
                exceptions = new List<Exception>();

            // delete all unrecognized index directories
            //foreach (var indexDirectory in new DirectoryInfo(path.FullPath).GetDirectories().Concat(indexesCustomPaths.Values.SelectMany(x => new DirectoryInfo(x).GetDirectories())))
            //{
            //    if (record.Indexes.ContainsKey(indexDirectory.Name) == false)
            //    {
            //        IOExtensions.DeleteDirectory(indexDirectory.FullName);

            //        continue;
            //    }

            //    // delete all redundant index instances
            //    var indexInstances = indexDirectory.GetDirectories();
            //    if (indexInstances.Length > 2)
            //    {
            //        var orderedIndexes = indexInstances.OrderByDescending(x =>
            //            int.Parse(x.Name.Substring(x.Name.LastIndexOf("\\") +1)));

            //        foreach (var indexToRemove in orderedIndexes.Skip(2))
            //        {
            //            Directory.Delete(indexToRemove.FullName);
            //        }
            //    }
            //}

            foreach (var kvp in record.Indexes)
            {
                if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                    return;

                var name = kvp.Key;
                var definition = kvp.Value;

                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(definition.Name);
                var indexPath = path.Combine(safeName).FullPath;
                if (Directory.Exists(indexPath))
                    OpenIndex(path, indexPath, exceptions, name, staticIndexDefinition: definition, autoIndexDefinition: null);
            }

            foreach (var kvp in record.AutoIndexes)
            {
                if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                    return;

                var name = kvp.Key;
                var definition = kvp.Value;

                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(definition.Name);
                var indexPath = path.Combine(safeName).FullPath;
                if (Directory.Exists(indexPath))
                    OpenIndex(path, indexPath, exceptions, name, staticIndexDefinition: null, autoIndexDefinition: definition);
            }

            if (exceptions != null && exceptions.Count > 0)
                throw new AggregateException("Could not load some of the indexes", exceptions);
        }

        public void OpenFaultyIndex(Index index)
        {
            Debug.Assert(index is FaultyInMemoryIndex);

            var path = _documentDatabase.Configuration.Indexing.StoragePath;
            var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name);
            var indexPath = path.Combine(safeName).FullPath;
            var exceptions = new List<Exception>();

            OpenIndex(path, indexPath, exceptions, index.Name, index.GetIndexDefinition(), null);

            if (exceptions.Count > 0)
            {
                // there will only one exception here
                throw exceptions.First();
            }

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded
                });
        }

        private void OpenIndex(PathSetting path, string indexPath, List<Exception> exceptions, string name, IndexDefinition staticIndexDefinition, AutoIndexDefinition autoIndexDefinition)
        {
            Index index = null;

            try
            {
                index = Index.Open(indexPath, _documentDatabase);

                var differences = IndexDefinitionCompareDifferences.None;

                if (staticIndexDefinition != null)
                {
                    if (staticIndexDefinition.LockMode != null && index.Definition.LockMode != staticIndexDefinition.LockMode)
                        differences |= IndexDefinitionCompareDifferences.LockMode;

                    if (staticIndexDefinition.Priority != null && index.Definition.Priority != staticIndexDefinition.Priority)
                        differences |= IndexDefinitionCompareDifferences.Priority;
                }
                else if (autoIndexDefinition != null)
                {
                    if (autoIndexDefinition.Priority != index.Definition.Priority)
                        differences |= IndexDefinitionCompareDifferences.Priority;
                }

                if (differences != IndexDefinitionCompareDifferences.None)
                {
                    // database record has different lock mode / priority setting than persisted locally

                    UpdateStaticIndexLockModeAndPriority(staticIndexDefinition, index, differences);
                }

                index.Start();
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Started {index.Name} from {indexPath}");

                _indexes.Add(index);
            }
            catch (Exception e)
            {
                var alreadyFaulted = _indexes.TryGetByName(name, out var i) &&
                                     i is FaultyInMemoryIndex;

                index?.Dispose();
                exceptions?.Add(e);

                if (alreadyFaulted)
                    return;

                var configuration = new FaultyInMemoryIndexConfiguration(path, _documentDatabase.Configuration);

                var fakeIndex = autoIndexDefinition != null
                    ? new FaultyInMemoryIndex(e, name, configuration, CreateAutoDefinition(autoIndexDefinition))
                    : new FaultyInMemoryIndex(e, name, configuration, staticIndexDefinition);

                var message = $"Could not open index at '{indexPath}'. Created in-memory, fake instance: {fakeIndex.Name}";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                _documentDatabase.NotificationCenter.Add(AlertRaised.Create(
                    _documentDatabase.Name,
                    "Indexes store initialization error",
                    message,
                    AlertType.IndexStore_IndexCouldNotBeOpened,
                    NotificationSeverity.Error,
                    key: fakeIndex.Name,
                    details: new ExceptionDetails(e)));
                _indexes.Add(fakeIndex);
            }
        }

        public IEnumerable<Index> GetIndexesForCollection(string collection)
        {
            return _indexes.GetForCollection(collection);
        }

        public IEnumerable<Index> GetIndexes()
        {
            return _indexes;
        }

        public void RunIdleOperations()
        {
            DatabaseRecord record;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                record = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);

            if (record.Topology.Members[0] != _serverStore.NodeTag)
                return;

            AsyncHelpers.RunSync(HandleUnusedAutoIndexes);
        }

        private async Task HandleUnusedAutoIndexes()
        {
            var timeToWaitBeforeMarkingAutoIndexAsIdle = _documentDatabase.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle;
            var timeToWaitBeforeDeletingAutoIndexMarkedAsIdle = _documentDatabase.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle;
            var ageThreshold = timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan.Add(timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan); // idle * 2

            var indexesSortedByLastQueryTime = (from index in _indexes
                                                where index.State != IndexState.Disabled && index.State != IndexState.Error
                                                let stats = index.GetStats()
                                                let lastQueryingTime = stats.LastQueryingTime ?? DateTime.MinValue
                                                orderby lastQueryingTime
                                                select new UnusedIndexState
                                                {
                                                    LastQueryingTime = lastQueryingTime,
                                                    Index = index,
                                                    State = stats.State,
                                                    CreationDate = stats.CreatedTimestamp
                                                }).ToList();

            for (var i = 0; i < indexesSortedByLastQueryTime.Count; i++)
            {
                var item = indexesSortedByLastQueryTime[i];

                if (item.Index.Type != IndexType.AutoMap && item.Index.Type != IndexType.AutoMapReduce)
                    continue;

                var now = _documentDatabase.Time.GetUtcNow();
                var age = now - item.CreationDate;
                var lastQuery = now - item.LastQueryingTime;

                if (item.State == IndexState.Normal)
                {
                    TimeSpan differenceBetweenNewestAndCurrentQueryingTime;
                    if (i < indexesSortedByLastQueryTime.Count - 1)
                    {
                        var lastItem = indexesSortedByLastQueryTime[indexesSortedByLastQueryTime.Count - 1];
                        differenceBetweenNewestAndCurrentQueryingTime = lastItem.LastQueryingTime - item.LastQueryingTime;
                    }
                    else
                    {
                        if (item.LastQueryingTime > _documentDatabase.StartTime)
                            differenceBetweenNewestAndCurrentQueryingTime = now - item.LastQueryingTime;
                        else
                            differenceBetweenNewestAndCurrentQueryingTime = TimeSpan.Zero;
                    }

                    if (differenceBetweenNewestAndCurrentQueryingTime >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                    {
                        if (lastQuery >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                        {
                            try
                            {
                                item.Index.SetState(IndexState.Idle);
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Changed index '{item.Index.Name}' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Failed to set state of '{item.Index.Name}' to {IndexState.Idle} when running idle operations", e);
                            }
                        }
                    }

                    continue;
                }

                if (item.State == IndexState.Idle)
                {
                    if (age <= ageThreshold || lastQuery >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                    {
                        await TryDeleteIndexIfExists(item.Index.Name);
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Deleted index '{item.Index.Name}' due to idleness. Age: {age}. Last query: {lastQuery}.");
                    }
                }
            }
        }

        private void InitializePath(PathSetting path)
        {
            if (Directory.Exists(path.FullPath) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                Directory.CreateDirectory(path.FullPath);
        }

        private static void ValidateAnalyzers(IndexDefinition definition)
        {
            if (definition.Fields == null)
                return;

            foreach (var kvp in definition.Fields)
            {
                if (string.IsNullOrWhiteSpace(kvp.Value.Analyzer))
                    continue;

                try
                {
                    IndexingExtensions.GetAnalyzerType(kvp.Key, kvp.Value.Analyzer);
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException(e.Message, e);
                }
            }
        }

        private class UnusedIndexState
        {
            public DateTime LastQueryingTime { get; set; }
            public Index Index { get; set; }
            public IndexState State { get; set; }
            public DateTime CreationDate { get; set; }
        }

        public bool TryReplaceIndexes(string oldIndexName, string replacementIndexName)
        {
            try
            {


                lock (GetIndexLock(oldIndexName))
                {
                    if (_indexes.TryGetByName(replacementIndexName, out Index newIndex) == false)
                        return true;

                    if (_indexes.TryGetByName(oldIndexName, out Index oldIndex))
                    {
                        oldIndexName = oldIndex.Name;

                        if (oldIndex.Type.IsStatic() && newIndex.Type.IsStatic())
                        {
                            var oldIndexDefinition = oldIndex.GetIndexDefinition();
                            var newIndexDefinition = newIndex.Definition.GetOrCreateIndexDefinitionInternal();

                            if (newIndex.Definition.LockMode == IndexLockMode.Unlock &&
                                newIndexDefinition.LockMode.HasValue == false &&
                                oldIndexDefinition.LockMode.HasValue)
                                newIndex.SetLock(oldIndexDefinition.LockMode.Value);

                            if (newIndex.Definition.Priority == IndexPriority.Normal &&
                                newIndexDefinition.Priority.HasValue == false &&
                                oldIndexDefinition.Priority.HasValue)
                                newIndex.SetPriority(oldIndexDefinition.Priority.Value);
                        }
                    }

                    _indexes.ReplaceIndex(oldIndexName, oldIndex, newIndex);

                    using (newIndex.DrainRunningQueries()) // to ensure nobody will start index meanwhile if we stop it here
                    {
                        var needToStop = newIndex.Status == IndexRunningStatus.Running && PoolOfThreads.LongRunningWork.Current != newIndex._indexingThread;

                        if (needToStop)
                        {
                            // stop the indexing to allow renaming the index 
                            // the write tx required to rename it might be hold by indexing thread
                            ExecuteIndexAction(() => newIndex.Stop());
                        }

                        try
                        {
                            newIndex.Rename(oldIndexName);
                        }
                        finally
                        {
                            if (needToStop)
                                ExecuteIndexAction(newIndex.Start);
                        }
                    }

                    newIndex.ResetIsSideBySideAfterReplacement();

                    if (oldIndex != null)
                    {
                        while (_documentDatabase.DatabaseShutdown.IsCancellationRequested == false)
                        {
                            try
                            {
                                using (oldIndex.DrainRunningQueries())
                                    DeleteIndexInternal(oldIndex, raiseNotification: false);

                                break;
                            }
                            catch (TimeoutException)
                            {
                            }
                        }
                    }

                    if (newIndex.Configuration.RunInMemory == false)
                    {
                        while (_documentDatabase.DatabaseShutdown.IsCancellationRequested == false)
                        {
                            try
                            {
                                using (newIndex.DrainRunningQueries())
                                {
                                    var oldIndexDirectoryName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(oldIndexName);
                                    var replacementIndexDirectoryName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(replacementIndexName);

                                    using (newIndex.RestartEnvironment())
                                    {
                                        IOExtensions.MoveDirectory(newIndex.Configuration.StoragePath.Combine(replacementIndexDirectoryName).FullPath,
                                            newIndex.Configuration.StoragePath.Combine(oldIndexDirectoryName).FullPath);

                                        if (newIndex.Configuration.TempPath != null)
                                        {
                                            IOExtensions.MoveDirectory(newIndex.Configuration.TempPath.Combine(replacementIndexDirectoryName).FullPath,
                                                newIndex.Configuration.TempPath.Combine(oldIndexDirectoryName).FullPath);
                                        }
                                    }
                                }

                                break;
                            }
                            catch (TimeoutException)
                            {
                            }
                        }
                    }

                    _documentDatabase.Changes.RaiseNotifications(
                        new IndexChange
                        {
                            Name = oldIndexName,
                            Type = IndexChangeTypes.SideBySideReplace
                        });

                    return true;
                }
            }
            catch (IOException)
            {
                // we do not want to try again, letting index to continue running
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Could not replace index '{oldIndexName}' with '{replacementIndexName}'.", e);

                return false;
            }
        }

        private object GetIndexLock(string name)
        {
            return _indexLocks.GetOrAdd(name, n => new object());
        }

        private void ExecuteIndexAction(Action action)
        {
            while (_documentDatabase.DatabaseShutdown.IsCancellationRequested == false)
            {
                try
                {
                    action();
                    break;
                }
                catch (TimeoutException)
                {
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Error during index replacement in the new index stop before renaming", e);

                    throw;
                }
            }
        }

        public async Task SetLock(string name, IndexLockMode mode)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            if (index.Type == IndexType.Faulty || index.Type.IsAuto())
            {
                index.SetLock(mode);  // this will throw proper exception
                return;
            }

            var command = new SetIndexLockCommand(name, mode, _documentDatabase.Name);

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public async Task SetPriority(string name, IndexPriority priority)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            var faultyInMemoryIndex = index as FaultyInMemoryIndex;
            if (faultyInMemoryIndex != null)
            {
                faultyInMemoryIndex.SetPriority(priority); // this will throw proper exception
                return;
            }

            var command = new SetIndexPriorityCommand(name, priority, _documentDatabase.Name);

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            await _documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public IndexMergeResults ProposeIndexMergeSuggestions()
        {
            var dic = new Dictionary<string, IndexDefinition>();

            foreach (var index in GetIndexes())
            {
                dic[index.Name] = index.GetIndexDefinition();
            }

            var indexMerger = new IndexMerger(dic);

            return indexMerger.ProposeIndexMergeSuggestions();
        }
    }

    public class CustomIndexPaths
    {
        public Dictionary<string, string> Paths;
    }
}
