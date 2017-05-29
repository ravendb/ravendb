using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Json;
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

        /// <summary>
        /// The current lock, used to make sure indexes/transformers have a unique names
        /// </summary>
        private readonly object _locker = new object();

        private readonly SemaphoreSlim _indexAndTransformerLocker;

        private bool _initialized;

        private bool _run = true;

        public readonly IndexIdentities Identities = new IndexIdentities();

        public Logger Logger => _logger;

        public IndexStore(DocumentDatabase documentDatabase, ServerStore serverStore, SemaphoreSlim indexAndTransformerLocker)
        {
            _documentDatabase = documentDatabase;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<IndexStore>(_documentDatabase.Name);
            _indexAndTransformerLocker = indexAndTransformerLocker;
        }

        public void HandleDatabaseRecordChange()
        {
            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                long etag;
                DatabaseRecord record;
                using (context.OpenReadTransaction())
                {
                    record = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name, out etag);
                    if (record == null)
                        return;
                }

                lock (_locker)
                {
                    HandleDeletes(record);
                    HandleChangesForStaticIndexes(record, etag);
                    HandleChangesForAutoIndexes(record);
                }
            }
        }

        private void HandleChangesForAutoIndexes(DatabaseRecord record)
        {
            foreach (var kvp in record.AutoIndexes)
            {
                var name = kvp.Key;
                var etag = kvp.Value.Etag;
                try
                {
                    var definition = CreateAutoDefinition(kvp.Value);

                    HandleAutoIndexChange(name, etag, definition);
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not create auto index {name}", e);
                }
            }
        }

        private void HandleAutoIndexChange(string name, long etag, IndexDefinitionBase definition)
        {
            var creationOptions = IndexCreationOptions.Create;
            var existingIndex = GetIndex(name);
            IndexDefinitionCompareDifferences differences =  IndexDefinitionCompareDifferences.None;
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
                
                differences &= ~IndexDefinitionCompareDifferences.Etag;

                
                if ((differences & IndexDefinitionCompareDifferences.LockMode) != 0)
                {
                    existingIndex.SetLock(definition.LockMode);    
                }

                if ((differences & IndexDefinitionCompareDifferences.Priority) != 0)
                {
                    existingIndex.SetPriority(definition.Priority);
                }
                
                existingIndex.Update(definition, existingIndex.Configuration);

                _indexes.UpdateIndexEtag(existingIndex.Etag, etag);

                return;
            }

            Index index;
            
            if (definition is AutoMapIndexDefinition)
                index = AutoMapIndex.CreateNew(etag, (AutoMapIndexDefinition)definition, _documentDatabase);
            else if (definition is AutoMapReduceIndexDefinition)
                index = AutoMapReduceIndex.CreateNew(etag, (AutoMapReduceIndexDefinition)definition, _documentDatabase);
            else
                throw new NotImplementedException($"Unknown index definition type: {definition.GetType().FullName}");

            CreateIndexInternal(index);
        }

        private static IndexDefinitionBase CreateAutoDefinition(AutoIndexDefinition definition)
        {
            var mapFields = definition
                .MapFields
                .Select(x =>
                {
                    var field = IndexField.Create(x.Key, x.Value, allFields: null);
                    field.MapReduceOperation = x.Value.MapReduceOperation;

                    return field;
                })
                .ToArray();

            if (definition.Type == IndexType.AutoMap)
            {
                var result = new AutoMapIndexDefinition(definition.Collection, mapFields);

                if (definition.LockMode.HasValue)
                    result.LockMode = definition.LockMode.Value;

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
                        var field = IndexField.Create(x.Key, x.Value, allFields: null);
                        field.MapReduceOperation = x.Value.MapReduceOperation;

                        return field;
                    })
                    .ToArray();

                var result = new AutoMapReduceIndexDefinition(definition.Collection, mapFields, groupByFields);
                if (definition.LockMode.HasValue)
                    result.LockMode = definition.LockMode.Value;

                if (definition.Priority.HasValue)
                    result.Priority = definition.Priority.Value;

                return result;
            }

            throw new NotSupportedException("Cannot create auto-index from " + definition.Type);
        }

        private void HandleChangesForStaticIndexes(DatabaseRecord record, long etag)
        {
            foreach (var kvp in record.Indexes)
            {
                var name = kvp.Key;
                var definition = kvp.Value;

                try
                {
                    HandleStaticIndexChange(name, definition);
                }
                catch (Exception exception)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not update static index {name}", exception);

                    var configuration = new FaultyInMemoryIndexConfiguration(_documentDatabase.Configuration.Indexing.StoragePath, _documentDatabase.Configuration);
                    var fakeIndex = new FaultyInMemoryIndex(exception, etag, name, configuration);
                    _indexes.Add(fakeIndex);
                }
            }
        }

        private void HandleStaticIndexChange(string name, IndexDefinition definition)
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
                var sideBySideIndex = GetIndex(replacementIndexName);
                if (sideBySideIndex != null)
                {
                    creationOptions = GetIndexCreationOptions(definition, sideBySideIndex, out IndexDefinitionCompareDifferences sideBySideDifferences);
                    if (creationOptions == IndexCreationOptions.Noop)
                        return;

                    if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                    {
                        UpdateIndex(definition, sideBySideIndex, sideBySideDifferences);
                    }
                }

                var replacementIndex = GetIndex(replacementIndexName);
                if (replacementIndex != null)
                    DeleteIndexInternal(replacementIndex);
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
        
        private void HandleDeletes(DatabaseRecord record)
        {
            foreach (var index in _indexes)
            {
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
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not delete index {index.Name}", e);
                }
            }
        }

        public Task InitializeAsync(DatabaseRecord record)
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

                InitializePath(_documentDatabase.Configuration.Indexing.StoragePath);

                _initialized = true;
            }

            return Task.Factory.StartNew(() =>
            {
                OpenIndexes(record);
                HandleDatabaseRecordChange();
            }, TaskCreationOptions.LongRunning);
        }

        public Index GetIndex(long etag)
        {
            Index index;
            if (_indexes.TryGetByEtag(etag, out index) == false)
                return null;

            return index;
        }

        public Index GetIndex(string name)
        {
            Index index;
            if (_indexes.TryGetByName(name, out index) == false)
                return null;

            return index;
        }

        public async Task<long> CreateIndex(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            ValidateIndexName(definition.Name);
            definition.RemoveDefaultValues();
            ValidateAnalyzers(definition);

            var instance = IndexAndTransformerCompilationCache.GetIndexInstance(definition); // pre-compile it and validate

            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                lock (_locker)
                {
                    if (definition.Type == IndexType.MapReduce)
                        MapReduceIndex.ValidateReduceResultsCollectionName(definition, instance, _documentDatabase);
                }

                var command = new PutIndexCommand(definition, _documentDatabase.Name);

                try
                {
                    var (etag, _) = await _serverStore.SendToLeaderAsync(command);
                    await _documentDatabase.WaitForIndexNotification(etag);

                    var index = GetIndex(definition.Name); // not all operations are changing Etag, this is why we need to take it directly from the index
                    if (index == null)
                        throw new InvalidOperationException("Failed to create index " + definition.Name);
                    return index.Etag;
                }
                catch (CommandExecutionException e)
                {
                    throw e.InnerException;
                }
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public async Task<long> CreateIndex(IndexDefinitionBase definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition)
                return await CreateIndex(((MapIndexDefinition)definition).IndexDefinition);

            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                ValidateIndexName(definition.Name);

                var command = PutAutoIndexCommand.Create(definition, _documentDatabase.Name);

                var (index, result) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.WaitForIndexNotification(index);

                var instance = GetIndex(definition.Name);

                return instance.Etag;
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public bool HasChanged(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            ValidateIndexName(definition.Name);

            var existingIndex = GetIndex(definition.Name);
            if (existingIndex == null)
                return true;

            var creationOptions = GetIndexCreationOptions(definition, existingIndex, out IndexDefinitionCompareDifferences differences);
            return creationOptions != IndexCreationOptions.Noop;
        }

        private void CreateIndexInternal(Index index)
        {
            Debug.Assert(index != null);
            Debug.Assert(index.Etag > 0);

            if (_documentDatabase.Configuration.Indexing.Disabled == false && _run)
                index.Start();

            _indexes.Add(index);

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded,
                    Etag = index.Etag
                });
        }

        private void UpdateIndex(IndexDefinition definition, Index existingIndex, IndexDefinitionCompareDifferences indexDifferences)
        {
            UpdateStaticIndexLockModeAndPriority(definition, existingIndex, indexDifferences);

            _indexes.UpdateIndexEtag(existingIndex.Etag, definition.Etag);

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

        private static IndexDefinitionCompareDifferences UpdateStaticIndexLockModeAndPriority(IndexDefinition definition, Index existingIndex,
            IndexDefinitionCompareDifferences indexDifferences)
        {
            indexDifferences &= ~IndexDefinitionCompareDifferences.Etag;

            if (definition.LockMode.HasValue && (indexDifferences & IndexDefinitionCompareDifferences.LockMode) != 0)
                existingIndex.SetLock(definition.LockMode.Value);

            if (definition.Priority.HasValue && (indexDifferences & IndexDefinitionCompareDifferences.Priority) != 0)
                existingIndex.SetPriority(definition.Priority.Value);

            indexDifferences &= ~IndexDefinitionCompareDifferences.LockMode;
            indexDifferences &= ~IndexDefinitionCompareDifferences.Priority;
            return indexDifferences;
        }

        internal IndexCreationOptions GetIndexCreationOptions(object indexDefinition, Index existingIndex)
        {
            return GetIndexCreationOptions(indexDefinition, existingIndex, out IndexDefinitionCompareDifferences differences);
        }


        internal IndexCreationOptions GetIndexCreationOptions(object indexDefinition, Index existingIndex, out IndexDefinitionCompareDifferences differences)
        {
            differences = IndexDefinitionCompareDifferences.All;
            if (existingIndex == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;
            differences = IndexDefinitionCompareDifferences.None;


            var indexDef = indexDefinition as IndexDefinition;
            if (indexDef != null)
                differences = existingIndex.Definition.Compare(indexDef);

            var indexDefBase = indexDefinition as IndexDefinitionBase;
            if (indexDefBase != null)
                differences = existingIndex.Definition.Compare(indexDefBase);

            if (differences == IndexDefinitionCompareDifferences.All)
                return IndexCreationOptions.Update;

            differences &= ~IndexDefinitionCompareDifferences.Etag; // we do not care about IndexId

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

        private void ValidateIndexName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Index name cannot be empty!");

            if (name.StartsWith(DynamicQueryRunner.DynamicIndexPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index names starting with dynamic_ or dynamic/ are reserved!", nameof(name));
            }

            if (name.Equals(DynamicQueryRunner.DynamicIndex, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index name dynamic is reserved!", nameof(name));
            }

            if (name.Contains("//"))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index name cannot contain // (double slashes)", nameof(name));
            }
        }

        public long ResetIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            return ResetIndexInternal(index);
        }

        public long ResetIndex(long etag)
        {
            var index = GetIndex(etag);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(etag);

            return ResetIndexInternal(index);
        }

        public async Task<bool> TryDeleteIndexIfExists(string name)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var index = GetIndex(name);
                if (index == null)
                    return false;

                var (etag, result) = await _serverStore.SendToLeaderAsync(new DeleteIndexCommand(index.Name, _documentDatabase.Name));

                await _documentDatabase.WaitForIndexNotification(etag);

                return true;
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public async Task DeleteIndex(long etag)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var index = GetIndex(etag);
                if (index == null)
                    IndexDoesNotExistException.ThrowFor(etag);

                var (resultEtag,result) = await _serverStore.SendToLeaderAsync(new DeleteIndexCommand(index.Name, _documentDatabase.Name));

                await _documentDatabase.WaitForIndexNotification(resultEtag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        private void DeleteIndexInternal(Index index)
        {
            lock (_locker)
            {
                Index _;
                _indexes.TryRemoveByEtag(index.Etag, out _);

                try
                {
                    index.Dispose();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not dispose index '{index.Name}' ({index.Etag}).", e);
                }

                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved,
                    Etag = index.Etag
                });

                if (index.Configuration.RunInMemory)
                    return;

                var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name);

                var indexPath = index.Configuration.StoragePath.Combine(name);

                var indexTempPath = index.Configuration.TempPath?.Combine(name);

                var journalPath = index.Configuration.JournalsStoragePath?.Combine(name);

                IOExtensions.DeleteDirectory(indexPath.FullPath);

                if (indexTempPath != null)
                    IOExtensions.DeleteDirectory(indexTempPath.FullPath);

                if (journalPath != null)
                    IOExtensions.DeleteDirectory(journalPath.FullPath);
            }
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

            index.Stop();

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

            Parallel.ForEach(indexes, index => index.Stop());

            foreach (var index in indexes)
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

        private long ResetIndexInternal(Index index)
        {
            DeleteIndexInternal(index);

            var definitionBase = index.Definition;
            if (definitionBase is AutoMapIndexDefinition)
                index = AutoMapIndex.CreateNew(index.Etag, (AutoMapIndexDefinition)definitionBase, _documentDatabase);
            else if (definitionBase is AutoMapReduceIndexDefinition)
                index = AutoMapReduceIndex.CreateNew(index.Etag, (AutoMapReduceIndexDefinition)definitionBase, _documentDatabase);
            else
            {
                var staticIndexDefinition = index.Definition.ConvertToIndexDefinition(index);
                switch (staticIndexDefinition.Type)
                {
                    case IndexType.Map:
                        index = MapIndex.CreateNew(staticIndexDefinition, _documentDatabase);
                        break;
                    case IndexType.MapReduce:
                        index = MapReduceIndex.CreateNew(staticIndexDefinition, _documentDatabase);
                        break;
                    default:
                        throw new NotSupportedException($"Cannot create {staticIndexDefinition.Type} index from IndexDefinition");
                }
            }

            CreateIndexInternal(index);

            return index.Etag;
        }

        private void OpenIndexes(DatabaseRecord record)
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_locker)
            {
                // apply renames
                OpenIndexesFromRecord(_documentDatabase.Configuration.Indexing.StoragePath, record);
            }
        }





        private void OpenIndexesFromRecord(PathSetting path, DatabaseRecord record)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Starting to load indexes from record");

            Dictionary<string, string> indexesCustomPaths;

            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var customPathsDoc = _documentDatabase.DocumentsStorage.Get(context, "Raven/CustomPaths");
                if (customPathsDoc != null)
                {
                    var customPaths = JsonDeserializationServer.CustomIndexPaths(customPathsDoc.Data);
                    indexesCustomPaths = customPaths.Paths;
                }
                else
                    indexesCustomPaths = new Dictionary<string, string>();
            }

            List<Exception> exceptions = null;
            if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
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
                var singleIndexConfiguration = new SingleIndexConfiguration(definition.Configuration, _documentDatabase.Configuration);
                var indexPath = path.Combine(safeName).FullPath;
                if (Directory.Exists(indexPath))
                    OpenIndex(path, definition.Etag, indexPath, exceptions, name);
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
                    OpenIndex(path, definition.Etag, indexPath, exceptions, name);
            }

            if (exceptions != null && exceptions.Count > 0)
                throw new AggregateException("Could not load some of the indexes", exceptions);
        }

        private void OpenIndex(PathSetting path, long etag, string indexPath, List<Exception> exceptions, string name)
        {
            Index index = null;

            try
            {
                index = Index.Open(etag, indexPath, _documentDatabase);
                index.Start();
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Started {index.Name} from {indexPath}");

                _indexes.Add(index);
            }
            catch (Exception e)
            {
                index?.Dispose();
                exceptions?.Add(e);

                var configuration = new FaultyInMemoryIndexConfiguration(path, _documentDatabase.Configuration);
                var fakeIndex = new FaultyInMemoryIndex(e, etag, name, configuration);

                var message = $"Could not open index with etag {etag} at '{indexPath}'. Created in-memory, fake instance: {fakeIndex.Name}";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Indexes store initialization error",
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
            AsyncHelpers.RunSync(HandleUnusedAutoIndexes);
            //DeleteSurpassedAutoIndexes(); // TODO [ppekrol]
        }

        private async Task HandleUnusedAutoIndexes()
        {
            if (_serverStore.IsLeader() == false)
                return;

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
                        differenceBetweenNewestAndCurrentQueryingTime = TimeSpan.Zero;

                    if (differenceBetweenNewestAndCurrentQueryingTime >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                    {
                        if (lastQuery >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                        {
                            item.Index.SetState(IndexState.Idle);
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Changed index '{item.Index.Name} ({item.Index.Etag})' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
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
                            _logger.Info($"Deleted index '{item.Index.Name} ({item.Index.Etag})' due to idleness. Age: {age}. Last query: {lastQuery}.");
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

        public bool TryReplaceIndexes(string oldIndexName, string newIndexName, bool immediately = false)
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(_locker, 16, ref lockTaken);
                if (lockTaken == false)
                    return false;

                Index newIndex;
                if (_indexes.TryGetByName(newIndexName, out newIndex) == false)
                    return true;

                Index oldIndex;
                if (_indexes.TryGetByName(oldIndexName, out oldIndex))
                {
                    oldIndexName = oldIndex.Name;

                    if (oldIndex.Type.IsStatic() && newIndex.Type.IsStatic())
                    {
                        var oldIndexDefinition = oldIndex.GetIndexDefinition();
                        var newIndexDefinition = newIndex.Definition.GetOrCreateIndexDefinitionInternal();

                        if (newIndex.Definition.LockMode == IndexLockMode.Unlock && newIndexDefinition.LockMode.HasValue == false && oldIndexDefinition.LockMode.HasValue)
                            newIndex.SetLock(oldIndexDefinition.LockMode.Value);

                        if (newIndex.Definition.Priority == IndexPriority.Normal && newIndexDefinition.Priority.HasValue == false && oldIndexDefinition.Priority.HasValue)
                            newIndex.SetPriority(oldIndexDefinition.Priority.Value);
                    }
                }

                _indexes.ReplaceIndex(oldIndexName, oldIndex, newIndex);
                newIndex.Rename(oldIndexName);

                if (oldIndex != null)
                {
                    using (oldIndex.DrainRunningQueries())
                        DeleteIndexInternal(oldIndex);
                }

                return true;
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_locker);
            }
        }

        public async Task SetLock(string name, IndexLockMode mode)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var index = GetIndex(name);
                if (index == null)
                    IndexDoesNotExistException.ThrowFor(name);

                var faultyInMemoryIndex = index as FaultyInMemoryIndex;
                if (faultyInMemoryIndex != null)
                {
                    faultyInMemoryIndex.SetLock(mode); // this will throw proper exception
                    return;
                }

                var command = new SetIndexLockCommand(name, mode, _documentDatabase.Name);

                var (etag, result) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.WaitForIndexNotification(etag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public async Task SetPriority(string name, IndexPriority priority)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
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

                var (etag, result) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.WaitForIndexNotification(etag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }
    }

    public class CustomIndexPaths
    {
        public Dictionary<string, string> Paths;
    }
}