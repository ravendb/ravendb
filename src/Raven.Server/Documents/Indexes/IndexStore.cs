using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Voron.Platform.Posix;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IndexStore));

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
            lock (_locker)
            {
                Index existingIndex;
                ValidateIndexDefinition(definition.Name, out existingIndex);

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
                        throw new NotSupportedException();
                    default:
                        throw new NotSupportedException($"Cannot create {definition.Type} index from IndexDefinition");
                }

                return CreateIndexInternal(index, indexId);
            }
        }

        public int CreateIndex(IndexDefinitionBase definition)
        {
            lock (_locker)
            {
                Index existingIndex;
                ValidateIndexDefinition(definition.Name, out existingIndex);

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

                var indexDefinition = definition as AutoMapIndexDefinition;
                if (indexDefinition != null)
                    index = AutoMapIndex.CreateNew(indexId, indexDefinition, _documentDatabase);
                else
                {
                    var reduceIndexDefinition = definition as AutoMapReduceIndexDefinition;
                    if (reduceIndexDefinition != null)
                        index = AutoMapReduceIndex.CreateNew(indexId, reduceIndexDefinition, _documentDatabase);
                    else
                        throw new NotImplementedException("Unknown index definition type: ");
                }

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

            _documentDatabase.Notifications.RaiseNotifications(
                new IndexChangeNotification { Name = index.Name, Type = IndexChangeTypes.IndexAdded });

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

        private void ValidateIndexDefinition(string name, out Index existingIndex)
        {
            ValidateIndexName(name);

            if (_indexes.TryGetByName(name, out existingIndex))
            {
                switch (existingIndex.Definition.LockMode)
                {
                    case IndexLockMode.SideBySide:
                        throw new NotImplementedException(); // TODO [ppekrol]
                    case IndexLockMode.LockedIgnore:
                        return;
                    case IndexLockMode.LockedError:
                        throw new InvalidOperationException("Can not overwrite locked index: " + name);
                }
            }
        }

        private void ValidateIndexName(string name)
        {
            if (name.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index names starting with dynamic_ or dynamic/ are reserved!", nameof(name));
            }

            if (name.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
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

        public void DeleteIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                throw new InvalidOperationException("There is no index with name: " + name);

            DeleteIndexInternal(index.IndexId);
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
                catch (Exception)
                {
                    //TODO [ppekrol] log
                }

                _documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved
                });

                if (_documentDatabase.Configuration.Indexing.RunInMemory)
                    return;

                var path = Path.Combine(_documentDatabase.Configuration.Indexing.IndexStoragePath, id.ToString());
                IOExtensions.DeleteDirectory(path);
            }
        }

        public void StartIndexing()
        {
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

            _run = true;

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

            _run = false;

            Parallel.ForEach(indexes, index => index.Stop());
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            var exceptionAggregator = new ExceptionAggregator(Log, $"Could not dispose {nameof(IndexStore)}");

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

                switch (index.Type)
                {
                    case IndexType.AutoMap:
                        var autoMapIndex = (AutoMapIndex)index;
                        var autoMapIndexDefinition = autoMapIndex.Definition;
                        return CreateIndex(autoMapIndexDefinition);
                    case IndexType.Map:
                        var staticMapIndex = (StaticMapIndex)index;
                        var staticMapIndexDefinition = staticMapIndex.Definition.IndexDefinition;
                        return CreateIndex(staticMapIndexDefinition);
                    default:
                        throw new NotSupportedException(index.Type.ToString());
                }
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
                    if (int.TryParse(indexDirectory.Name, out indexId) == false)
                        continue;

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Indexing.ThrowIfAnyIndexCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    try
                    {
                        var index = Index.Open(indexId, _documentDatabase);
                        _indexes.Add(index);
                    }
                    catch (Exception e)
                    {
                        exceptions?.Add(e);

                        // TODO arek: I think we can ignore auto indexes here, however for static ones try to retrieve names
                        var fakeIndex = new FaultyInMemoryIndex(indexId, IndexDefinitionBase.TryReadNameFromMetadataFile(indexDirectory));

                        Log.ErrorException($"Could not open index with id {indexId}. Created in-memory, fake instance: {fakeIndex.Name}", e);
                        // TODO arek: add alert

                        _indexes.Add(fakeIndex);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the indexes", exceptions);
                }
            }
        }

        public List<IndexDefinitionBase> GetIndexDefinitionsForCollection(string collection, IndexType type)
        {
            return _indexes
                .GetForCollection(collection)
                .Where(x => x.Type == type)
                .Select(x => x.Definition)
                .ToList();
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

                var age = SystemTime.UtcNow - item.CreationDate;
                var lastQuery = SystemTime.UtcNow - item.LastQueryingTime;

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
                            Log.Warn($"Changed index '{item.Index.Name} ({item.Index.IndexId})' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
                        }
                    }

                    continue;
                }

                if (item.Priority.HasFlag(IndexingPriority.Idle))
                {
                    if (age <= ageThreshold || lastQuery >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                    {
                        DeleteIndex(item.Index.IndexId);
                        Log.Warn($"Deleted index '{item.Index.Name} ({item.Index.IndexId})' due to idleness. Age: {age}. Last query: {lastQuery}.");
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