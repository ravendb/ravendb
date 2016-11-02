using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Utils;

using Voron.Platform.Posix;

using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {

        private static Logger _logger;
        private readonly DocumentDatabase _documentDatabase;

        private readonly CollectionOfIndexes _indexes = new CollectionOfIndexes();

        private readonly object _locker = new object();

        private bool _initialized;

        private string _path;
        private bool _run = true;

        public readonly IndexIdentities Identities = new IndexIdentities();

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
                    _path = _documentDatabase.Configuration.Indexing.IndexStoragePath;

                    if (Platform.RunningOnPosix)
                        _path = PosixHelper.FixLinuxPath(_path);

                    if (Directory.Exists(_path) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                        Directory.CreateDirectory(_path);
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

                switch (GetIndexCreationOptions(definition, existingIndex))
                {
                    case IndexCreationOptions.Noop:
                        return existingIndex.IndexId;
                    case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
                        throw new NotImplementedException(); // TODO [ppekrol]
                    case IndexCreationOptions.Update:
                        DeleteIndex(existingIndex.IndexId);
                        break;
                }

                var indexId = _indexes.GetNextIndexId();

                Index index;

                switch (definition.Type)
                {
                    case IndexType.Map:
                        index = StaticMapIndex.CreateNew(indexId, definition, _documentDatabase);
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
                        throw new NotImplementedException(); // TODO [ppekrol]
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
                else if (definition is StaticMapIndexDefinition)
                {
                    var mapReduceIndexDef = definition as MapReduceIndexDefinition;

                    if (mapReduceIndexDef != null)
                    {
                        index = MapReduceIndex.CreateNew(indexId, ((MapReduceIndexDefinition)definition).IndexDefinition, _documentDatabase);
                    }
                    else
                    {
                        index = StaticMapIndex.CreateNew(indexId, ((StaticMapIndexDefinition)definition).IndexDefinition, _documentDatabase);
                    }
                }
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

            var etag = _documentDatabase.IndexesPersistence.OnIndexCreated(index);

            _documentDatabase.Notifications.RaiseNotifications(
                new IndexChangeNotification
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded,
                    Etag = etag
                });

            return indexId;
        }

        private static IndexCreationOptions GetIndexCreationOptions(IndexDefinition indexDefinition, Index existingIndex)
        {
            // TODO [ppekrol] remove code duplication

            if (existingIndex == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;

            var equals = existingIndex.Definition.Equals(indexDefinition, ignoreFormatting: true, ignoreMaxIndexOutputs: true);
            if (equals)
                return IndexCreationOptions.Noop;

            return existingIndex.Definition.Equals(indexDefinition, ignoreFormatting: true, ignoreMaxIndexOutputs: true)
                       ? IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex
                       : IndexCreationOptions.Update;
        }

        private static IndexCreationOptions GetIndexCreationOptions(IndexDefinitionBase indexDefinition, Index existingIndex)
        {
            // TODO [ppekrol] remove code duplication

            if (existingIndex == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;

            var equals = existingIndex.Definition.Equals(indexDefinition, ignoreFormatting: true, ignoreMaxIndexOutputs: true);
            if (equals)
                return IndexCreationOptions.Noop;

            return existingIndex.Definition.Equals(indexDefinition, ignoreFormatting: true, ignoreMaxIndexOutputs: true)
                       ? IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex
                       : IndexCreationOptions.Update;
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
                throw new InvalidOperationException("There is no index with name: " + name);

            return ResetIndexInternal(index);
        }

        public int ResetIndex(int id)
        {
            var index = GetIndex(id);
            if (index == null)
                throw new InvalidOperationException("There is no index with id: " + id);

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
                    throw new InvalidOperationException("There is no index with id: " + id);

                try
                {
                    index.Dispose();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not dispose index '{index.Name}' ({id}).", e);
                }

                _documentDatabase.IndexesPersistence.OnIndexDeleted(index);


                _documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved
                });

                if (_documentDatabase.Configuration.Indexing.RunInMemory)
                    return;

                var path = Path.Combine(_documentDatabase.Configuration.Indexing.IndexStoragePath,
                    index.GetIndexNameSafeForFileSystem());
                IOExtensions.DeleteDirectory(path);
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
                throw new InvalidOperationException("There is no index with name: " + name);

            index.Start();
        }

        public void StopIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                throw new InvalidOperationException("There is no index with name: " + name);

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
                foreach (var indexDirectory in new DirectoryInfo(_path).GetDirectories())
                {
                    if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                        return;

                    int indexId;
                    string indexName;
                    if (IndexDefinitionBase.TryReadIdFromDirectory(indexDirectory, out indexId, out indexName) == false)
                        continue;

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    Index index = null;

                    try
                    {
                        index = Index.Open(indexId, indexDirectory.FullName, _documentDatabase);
                        index.Start();
                        _indexes.Add(index);
                    }
                    catch (Exception e)
                    {
                        index?.Dispose();
                        exceptions?.Add(e);

                        var fakeIndex = new FaultyInMemoryIndex(e,indexId, IndexDefinitionBase.TryReadNameFromMetadataFile(indexDirectory) ?? indexName);

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Could not open index with id {indexId}. Created in-memory, fake instance: {fakeIndex.Name}", e);
                        // TODO arek: add alert

                        _indexes.Add(fakeIndex);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the indexes", exceptions);
                }
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
                                                where index.Priority.HasFlag(IndexingPriority.Disabled) == false && index.Priority.HasFlag(IndexingPriority.Error) == false && index.Priority.HasFlag(IndexingPriority.Forced) == false
                                                let stats = index.GetStats()
                                                let lastQueryingTime = stats.LastQueryingTime ?? DateTime.MinValue
                                                orderby lastQueryingTime
                                                select new UnusedIndexState
                                                {
                                                    LastQueryingTime = lastQueryingTime,
                                                    Index = index,
                                                    Priority = stats.Priority,
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

                if (item.Priority.HasFlag(IndexingPriority.Normal))
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
                            item.Index.SetPriority(IndexingPriority.Idle);
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Changed index '{item.Index.Name} ({item.Index.IndexId})' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
                        }
                    }

                    continue;
                }

                if (item.Priority.HasFlag(IndexingPriority.Idle))
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

        private class UnusedIndexState
        {
            public DateTime LastQueryingTime { get; set; }
            public Index Index { get; set; }
            public IndexingPriority Priority { get; set; }
            public DateTime CreationDate { get; set; }
        }
    }
}