using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;

using Voron.Platform.Posix;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _documentDatabase;

        private readonly CollectionOfIndexes _indexes = new CollectionOfIndexes();

        private readonly object _locker = new object();

        private bool _initialized;

        private bool _run = true;

        public readonly IndexIdentities Identities = new IndexIdentities();

        public Logger Logger => _logger;

        public IndexStore(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<IndexStore>(_documentDatabase.Name);
        }

        public Task InitializeAsync()
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

                if (_documentDatabase.Configuration.Indexing.RunInMemory == false)
                {
                    InitializePath(_documentDatabase.Configuration.Indexing.StoragePath);

                    if (_documentDatabase.Configuration.Indexing.AdditionalStoragePaths != null)
                    {
                        foreach (var path in _documentDatabase.Configuration.Indexing.AdditionalStoragePaths)
                            InitializePath(path);
                    }
                }

                _initialized = true;

                return Task.Factory.StartNew(OpenIndexes, TaskCreationOptions.LongRunning);
            }
        }

        public Index GetIndex(int id)
        {
            Index index;
            if (_indexes.TryGetById(id, out index) == false)
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

        public int CreateIndex(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            lock (_locker)
            {
                Index existingIndex;
                var lockMode = ValidateIndexDefinition(definition.Name, out existingIndex);
                if (lockMode == IndexLockMode.LockedIgnore)
                    return existingIndex.IndexId;

                definition.RemoveDefaultValues();

                switch (GetIndexCreationOptions(definition, existingIndex))
                {
                    case IndexCreationOptions.Noop:
                        return existingIndex.IndexId;
                    case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
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
                        return existingIndex.IndexId;
                    case IndexCreationOptions.Update:
                        DeleteIndex(existingIndex.IndexId);
                        break;
                }

                var indexId = _indexes.GetNextIndexId();

                Index index;

                switch (definition.Type)
                {
                    case IndexType.Map:
                        index = MapIndex.CreateNew(indexId, definition, _documentDatabase);
                        break;
                    case IndexType.MapReduce:
                        index = MapReduceIndex.CreateNew(indexId, definition, _documentDatabase);
                        break;
                    default:
                        throw new NotSupportedException($"Cannot create {definition.Type} index from IndexDefinition");
                }

                return CreateIndexInternal(index, indexId);
            }
        }

        public int CreateIndex(IndexDefinitionBase definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition)
                return CreateIndex(((MapIndexDefinition)definition).IndexDefinition);

            lock (_locker)
            {
                Index existingIndex;
                var lockMode = ValidateIndexDefinition(definition.Name, out existingIndex);
                if (lockMode == IndexLockMode.LockedIgnore)
                    return existingIndex.IndexId;

                switch (GetIndexCreationOptions(definition, existingIndex))
                {
                    case IndexCreationOptions.Noop:
                        return existingIndex.IndexId;
                    case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
                        throw new NotSupportedException();
                    case IndexCreationOptions.Update:
                        DeleteIndex(existingIndex.IndexId);
                        break;
                }

                var indexId = _indexes.GetNextIndexId();

                Index index;

                if (definition is AutoMapIndexDefinition)
                    index = AutoMapIndex.CreateNew(indexId, (AutoMapIndexDefinition)definition, _documentDatabase);
                else if (definition is AutoMapReduceIndexDefinition)
                    index = AutoMapReduceIndex.CreateNew(indexId, (AutoMapReduceIndexDefinition)definition, _documentDatabase);
                else
                    throw new NotImplementedException($"Unknown index definition type: {definition.GetType().FullName}");

                return CreateIndexInternal(index, indexId);
            }
        }

        private int CreateIndexInternal(Index index, int indexId)
        {
            Debug.Assert(index != null);
            Debug.Assert(indexId > 0);

            if (_documentDatabase.Configuration.Indexing.Disabled == false && _run)
                index.Start();

            _indexes.Add(index);

            var etag = _documentDatabase.IndexMetadataPersistence.OnIndexCreated(index);

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded,
                    Etag = etag
                });

            return indexId;
        }

        internal IndexCreationOptions GetIndexCreationOptions(object indexDefinition, Index existingIndex)
        {
            if (existingIndex == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;

            var result = IndexDefinitionCompareDifferences.None;

            var indexDef = indexDefinition as IndexDefinition;
            if (indexDef != null)
                result = existingIndex.Definition.Compare(indexDef);

            var indexDefBase = indexDefinition as IndexDefinitionBase;
            if (indexDefBase != null)
                result = existingIndex.Definition.Compare(indexDefBase);

            if (result == IndexDefinitionCompareDifferences.All)
                return IndexCreationOptions.Update;

            result &= ~IndexDefinitionCompareDifferences.IndexId; // we do not care about IndexId

            if (result == IndexDefinitionCompareDifferences.None)
                return IndexCreationOptions.Noop;

            if (result.HasFlag(IndexDefinitionCompareDifferences.Maps) || result.HasFlag(IndexDefinitionCompareDifferences.Reduce))
                return IndexCreationOptions.Update;

            if (result.HasFlag(IndexDefinitionCompareDifferences.Fields))
                return IndexCreationOptions.Update;

            if (result.HasFlag(IndexDefinitionCompareDifferences.Configuration))
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

            if (result.HasFlag(IndexDefinitionCompareDifferences.MapsFormatting) || result.HasFlag(IndexDefinitionCompareDifferences.ReduceFormatting))
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            return IndexCreationOptions.Update;
        }

        private IndexLockMode ValidateIndexDefinition(string name, out Index existingIndex)
        {
            ValidateIndexName(name);

            if (_indexes.TryGetByName(name, out existingIndex))
            {
                switch (existingIndex.Definition.LockMode)
                {
                    case IndexLockMode.SideBySide:
                        throw new NotImplementedException(); // TODO [ppekrol]
                    case IndexLockMode.LockedIgnore:
                        return IndexLockMode.LockedIgnore;
                    case IndexLockMode.LockedError:
                        throw new InvalidOperationException("Can not overwrite locked index: " + name);
                }
            }

            return IndexLockMode.Unlock;
        }

        private void ValidateIndexName(string name)
        {
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

        public int ResetIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistsException.ThrowFor(name);

            return ResetIndexInternal(index);
        }

        public int ResetIndex(int id)
        {
            var index = GetIndex(id);
            if (index == null)
                IndexDoesNotExistsException.ThrowFor(id);

            return ResetIndexInternal(index);
        }

        public bool TryDeleteIndexIfExists(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                return false;

            DeleteIndexInternal(index.IndexId);
            return true;
        }

        public void DeleteIndex(int id)
        {
            DeleteIndexInternal(id);
        }

        private void DeleteIndexInternal(int id)
        {
            lock (_locker)
            {
                Index index;
                if (_indexes.TryRemoveById(id, out index) == false)
                    IndexDoesNotExistsException.ThrowFor(id);

                try
                {
                    index.Dispose();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not dispose index '{index.Name}' ({id}).", e);
                }

                var tombstoneEtag = _documentDatabase.IndexMetadataPersistence.OnIndexDeleted(index);
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved,
                    Etag = tombstoneEtag
                });

                if (index.Configuration.RunInMemory)
                    return;

                var name = index.GetIndexNameSafeForFileSystem();

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
                IndexDoesNotExistsException.ThrowFor(name);

            index.Start();
        }

        public void StopIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistsException.ThrowFor(name);

            index.Stop();
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
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(IndexStore)}");

            foreach (var index in _indexes)
            {
                if (index is FaultyInMemoryIndex)
                    continue;

                exceptionAggregator.Execute(() =>
                {
                    index.Dispose();
                });
            }

            exceptionAggregator.ThrowIfNeeded();
        }

        private int ResetIndexInternal(Index index)
        {
            lock (_locker)
            {
                DeleteIndex(index.IndexId);
                return CreateIndex(index.Definition);
            }
        }

        private void OpenIndexes()
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_locker)
            {
                OpenIndexesFromDirectory(_documentDatabase.Configuration.Indexing.StoragePath);

                if (_documentDatabase.Configuration.Indexing.AdditionalStoragePaths != null)
                {
                    foreach (var path in _documentDatabase.Configuration.Indexing.AdditionalStoragePaths)
                        OpenIndexesFromDirectory(path);
                }
            }
        }

        private void OpenIndexesFromDirectory(PathSetting path)
        {
            if (Directory.Exists(path.FullPath) == false)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Starting to load indexes from {path}");

            var indexes = new SortedList<int, Tuple<string, string>>();
            foreach (var indexDirectory in new DirectoryInfo(path.FullPath).GetDirectories())
            {
                if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                    return;

                int indexId;
                string indexName;
                if (IndexDefinitionBase.TryReadIdFromDirectory(indexDirectory, out indexId, out indexName) == false)
                    continue;

                indexes[indexId] = Tuple.Create(indexDirectory.FullName, indexName);
            }

            foreach (var indexDirectory in indexes)
            {
                if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                    return;

                var indexId = indexDirectory.Key;
                var indexName = indexDirectory.Value.Item2;
                var indexPath = indexDirectory.Value.Item1;

                List<Exception> exceptions = null;
                if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                    exceptions = new List<Exception>();

                Index _;
                if (_indexes.TryGetById(indexId, out _))
                {
                    var message = $"Could not open index with id {indexId} at '{indexPath}'. Index with the same id already exists.";

                    exceptions?.Add(new InvalidOperationException(message));

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message);
                }
                else
                {
                    Index index = null;

                    try
                    {
                        index = Index.Open(indexId, indexPath, _documentDatabase);
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
                        var fakeIndex = new FaultyInMemoryIndex(e, indexId, IndexDefinitionBase.TryReadNameFromMetadataFile(indexPath) ?? indexName, configuration);
                        
                        var message = $"Could not open index with id {indexId} at '{indexPath}'. Created in-memory, fake instance: {fakeIndex.Name}";

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

                if (exceptions != null && exceptions.Count > 0)
                    throw new AggregateException("Could not load some of the indexes", exceptions);
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
            HandleUnusedAutoIndexes();
            //DeleteSurpassedAutoIndexes(); // TODO [ppekrol]
        }

        private void HandleUnusedAutoIndexes()
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
                        differenceBetweenNewestAndCurrentQueryingTime = TimeSpan.Zero;

                    if (differenceBetweenNewestAndCurrentQueryingTime >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                    {
                        if (lastQuery >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                        {
                            item.Index.SetState(IndexState.Idle);
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Changed index '{item.Index.Name} ({item.Index.IndexId})' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
                        }
                    }

                    continue;
                }

                if (item.State == IndexState.Idle)
                {
                    if (age <= ageThreshold || lastQuery >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                    {
                        DeleteIndex(item.Index.IndexId);
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Deleted index '{item.Index.Name} ({item.Index.IndexId})' due to idleness. Age: {age}. Last query: {lastQuery}.");
                    }
                }
            }
        }

        private void InitializePath(PathSetting path)
        {
            if (Directory.Exists(path.FullPath) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                Directory.CreateDirectory(path.FullPath);
        }

        private class UnusedIndexState
        {
            public DateTime LastQueryingTime { get; set; }
            public Index Index { get; set; }
            public IndexState State { get; set; }
            public DateTime CreationDate { get; set; }
        }
    }
}