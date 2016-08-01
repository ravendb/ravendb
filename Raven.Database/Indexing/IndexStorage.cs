//-----------------------------------------------------------------------
// <copyright file="IndexStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Constants = Raven.Abstractions.Data.Constants;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Indexing
{
    /// <summary>
    /// 	Thread safe, single instance for the entire application
    /// </summary>
    public class IndexStorage : CriticalFinalizerObject, IDisposable
    {
        private readonly DocumentDatabase documentDatabase;
        private const string IndexVersion = "2.0.0.1";
        private const string MapReduceIndexVersion = "2.5.0.1";

        private readonly IndexDefinitionStorage indexDefinitionStorage;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly string path;
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private static readonly ILog startupLog = LogManager.GetLogger(typeof(IndexStorage).FullName + ".Startup");
        private readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();
        private DateTime latestPersistedQueryTime;
        private readonly FileStream crashMarker;
        private ConcurrentDictionary<int, Index> indexes =
            new ConcurrentDictionary<int, Index>();

        public class RegisterLowMemoryHandler : ILowMemoryHandler
        {
            static RegisterLowMemoryHandler _instance;

            public static void Setup()
            {
                if (_instance != null)
                    return;
                lock (typeof(RegisterLowMemoryHandler))
                {
                    if (_instance != null)
                        return;
                    _instance = new RegisterLowMemoryHandler();
                    MemoryStatistics.RegisterLowMemoryHandler(_instance);
                }
            }

            public LowMemoryHandlerStatistics HandleLowMemory()
            {
                FieldCache_Fields.DEFAULT.PurgeAllCaches();
                var stat = GetStats();
                return new LowMemoryHandlerStatistics
                {
                    Name = stat.Name,
                    DatabaseName = stat.DatabaseName,
                    Summary = "Field Cache forcibly expunges all entries from the underlying caches"
                };
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                var cacheEntries = FieldCache_Fields.DEFAULT.GetCacheEntries();
                var memorySum = cacheEntries.Sum(x =>
                {
                    var curEstimator = new RamUsageEstimator(false);
                    return curEstimator.EstimateRamUsage(x);
                });
                return new LowMemoryHandlerStatistics
                {
                    Name = "LuceneLowMemoryHandler",
                    EstimatedUsedMemory = memorySum,
                    Metadata = new
                    {
                        CachedEntriesAmount = cacheEntries.Length
                    }
                };
            }
        }

        public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
        {
            try
            {
                RegisterLowMemoryHandler.Setup();
                this.indexDefinitionStorage = indexDefinitionStorage;
                this.configuration = configuration;
                this.documentDatabase = documentDatabase;
                path = configuration.IndexStoragePath;

                if (System.IO.Directory.Exists(path) == false && configuration.RunInMemory == false)
                    System.IO.Directory.CreateDirectory(path);

                if (configuration.RunInMemory == false)
                {
                    var crashMarkerPath = Path.Combine(path, "indexing.crash-marker");

                    if (File.Exists(crashMarkerPath))
                    {
                        // the only way this can happen is if we crashed because of a power outage
                        // in this case, we consider all open indexes to be corrupt and force them
                        // to be reset. This is because to get better perf, we don't flush the files to disk,
                        // so in the case of a power outage, we can't be sure that there wasn't still stuff in
                        // the OS buffer that wasn't written yet.
                        configuration.ResetIndexOnUncleanShutdown = true;
                    }

                    // The delete on close ensures that the only way this file will exists is if there was
                    // a power outage while the server was running.
                    crashMarker = File.Create(crashMarkerPath, 16, FileOptions.DeleteOnClose);
                }

                if (log.IsDebugEnabled)
                    log.Debug("Start opening indexes. There are {0} indexes that need to be loaded", indexDefinitionStorage.IndexNames.Length);

                BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(documentDatabase.WorkContext, indexDefinitionStorage.IndexNames,
                    name =>
                    {
                        var index = OpenIndex(name, onStartup: true, forceFullIndexCheck: false);

                        if (index != null)
                            indexes.TryAdd(index.IndexId, index);
                        if (startupLog.IsDebugEnabled)
                            startupLog.Debug("{0}/{1} indexes loaded", indexes.Count, indexDefinitionStorage.IndexNames.Length);
                    });
                if (log.IsDebugEnabled)
                    log.Debug("Index storage initialized. All indexes have been opened.");
            }
            catch (Exception e)
            {
                log.WarnException("Could not create index storage", e);
                try
                {
                    Dispose();
                }
                catch (Exception ex)
                {
                    log.FatalException("Failed to dispose when already getting an error during ctor", ex);
                }
                throw;
            }
        }

        private Index OpenIndex(string indexName, bool onStartup, bool forceFullIndexCheck)
        {
            if (indexName == null)
                throw new ArgumentNullException("indexName");
            if (startupLog.IsDebugEnabled)
                startupLog.Debug("Loading saved index {0}", indexName);

            var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexName);
            if (indexDefinition == null)
                return null;

            Index indexImplementation = null;
            bool resetTried = false;
            bool recoveryTried = false;
            string[] keysToDeleteAfterRecovery = null;
            while (true)
            {
                Directory luceneDirectory = null;
                try
                {
                    luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition, createIfMissing: resetTried, forceFullExistingIndexCheck: forceFullIndexCheck);
                    indexImplementation = CreateIndexImplementation(indexDefinition, luceneDirectory);

                    CheckIndexState(luceneDirectory, indexDefinition, indexImplementation, resetTried);

                    if (forceFullIndexCheck)
                    {
                        // the above index check might pass however an index writer creation can still throw an exception
                        // so we need to check it here to avoid crashing in runtime
                        new IndexWriter(luceneDirectory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
                    }

                    var simpleIndex = indexImplementation as SimpleIndex; // no need to do this on m/r indexes, since we rebuild them from saved data anyway
                    if (simpleIndex != null && keysToDeleteAfterRecovery != null)
                    {
                        // remove keys from index that were deleted after creating commit point
                        simpleIndex.RemoveDirectlyFromIndex(keysToDeleteAfterRecovery, GetLastEtagForIndex(simpleIndex));
                    }

                    LoadExistingSuggestionsExtentions(indexDefinition.Name, indexImplementation);

                    documentDatabase.TransactionalStorage.Batch(accessor =>
                    {
                        IndexStats indexStats = accessor.Indexing.GetIndexStats(indexDefinition.IndexId);
                        if (indexStats != null)
                            indexImplementation.Priority = indexStats.Priority;

                        var read = accessor.Lists.Read("Raven/Indexes/QueryTime", indexName);
                        if (read == null)
                        {
                            if (IsIdleAutoIndex(indexImplementation))
                                indexImplementation.MarkQueried(); // prevent index abandoning right after startup

                            return;
                        }

                        var dateTime = read.Data.Value<DateTime>("LastQueryTime");

                        if (IsIdleAutoIndex(indexImplementation) && SystemTime.UtcNow - dateTime > configuration.TimeToWaitBeforeRunningAbandonedIndexes)
                            indexImplementation.MarkQueried(); // prevent index abandoning right after startup
                        else
                            indexImplementation.MarkQueried(dateTime);

                        if (dateTime > latestPersistedQueryTime)
                            latestPersistedQueryTime = dateTime;
                    });

                    break;
                }
                catch (Exception e)
                {
                    if (resetTried)
                        throw new InvalidOperationException("Could not open / create index" + indexName + ", reset already tried", e);

                    if (indexImplementation != null)
                        indexImplementation.Dispose();

                    if (recoveryTried == false && luceneDirectory != null && configuration.Indexing.SkipRecoveryOnStartup == false)
                    {
                        recoveryTried = true;
                        startupLog.WarnException("Could not open index " + indexName + ". Trying to recover index", e);

                        keysToDeleteAfterRecovery = TryRecoveringIndex(indexDefinition, luceneDirectory);
                    }
                    else
                    {
                        resetTried = true;
                        startupLog.WarnException("Could not open index " + indexName + ". Recovery operation failed, forcibly resetting index", e);
                        TryResettingIndex(indexName, indexDefinition, onStartup);
                    }
                }
            }

            return indexImplementation;
        }

        private void CheckIndexState(Directory directory, IndexDefinition indexDefinition, Index index, bool resetTried)
        {
            //if (configuration.ResetIndexOnUncleanShutdown == false)
            //	return;

            // 1. If commitData is null it means that there were no commits, so just in case we are resetting to Etag.Empty
            // 2. If no 'LastEtag' in commitData then we consider it an invalid index
            // 3. If 'LastEtag' is present (and valid), then resetting to it (if it is lower than lastStoredEtag)

            var commitData = IndexReader.GetCommitUserData(directory);

            if (index.IsMapReduce)
                CheckMapReduceIndexState(commitData, resetTried);
            else
                CheckMapIndexState(commitData, indexDefinition, index);
        }

        private void CheckMapIndexState(IDictionary<string, string> commitData, IndexDefinition indexDefinition, Index index)
        {
            string value;
            Etag lastEtag = null;
            if (commitData != null && commitData.TryGetValue("LastEtag", out value))
                Etag.TryParse(value, out lastEtag); // etag will be null if parsing will fail

            var lastStoredEtag = GetLastEtagForIndex(index) ?? Etag.Empty;
            lastEtag = lastEtag ?? Etag.Empty;

            if (EtagUtil.IsGreaterThanOrEqual(lastEtag, lastStoredEtag))
                return;

            log.Info(string.Format("Resetting index '{0} ({1})'. Last stored etag: {2}. Last commit etag: {3}.", indexDefinition.Name, index.indexId, lastStoredEtag, lastEtag));

            var now = SystemTime.UtcNow;
            ResetLastIndexedEtag(indexDefinition, lastEtag, now);
        }

        private static void CheckMapReduceIndexState(IDictionary<string, string> commitData, bool resetTried)
        {
            if (resetTried)
                return;

            string marker;
            long commitMarker;
            var valid = commitData != null
                && commitData.TryGetValue("Marker", out marker)
                && long.TryParse(marker, out commitMarker)
                && commitMarker == RavenIndexWriter.CommitMarker;

            if (valid == false)
                throw new InvalidOperationException("Map-Reduce index corruption detected.");
        }

        private static bool IsIdleAutoIndex(Index index)
        {
            return index.PublicName.StartsWith("Auto/") && index.Priority == IndexingPriority.Idle;
        }

        private void TryResettingIndex(string indexName, IndexDefinition indexDefinition, bool onStartup)
        {
            try
            {
                Action reset = () =>
                {
                    try
                    {
                        documentDatabase.Indexes.DeleteIndex(indexDefinition, removeIndexReplaceDocument: false);
                        documentDatabase.Indexes.PutNewIndexIntoStorage(indexName, indexDefinition);

                        var indexReplaceDocumentKey = Constants.IndexReplacePrefix + indexName;
                        var indexReplaceDocument = documentDatabase.Documents.Get(indexReplaceDocumentKey, null);
                        if (indexReplaceDocument == null)
                            return;

                        documentDatabase.Documents.Put(indexReplaceDocumentKey, null, indexReplaceDocument.DataAsJson, indexReplaceDocument.Metadata, null);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not finalize reseting of index: " + indexName, e);
                    }
                };

                if (onStartup)
                {
                    // we have to defer the work here until the database is actually ready for work
                    documentDatabase.OnIndexingWiringComplete += reset;
                }
                else
                {
                    reset();
                }

                var indexFullPath = Path.Combine(path, indexDefinition.IndexId.ToString(CultureInfo.InvariantCulture));
                IOExtensions.DeleteDirectory(indexFullPath);

                var suggestionsForIndex = Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName);
                IOExtensions.DeleteDirectory(suggestionsForIndex);

            }
            catch (Exception exception)
            {
                throw new InvalidOperationException("Could not reset index " + indexName, exception);
            }
        }

        private string[] TryRecoveringIndex(IndexDefinition indexDefinition,
                                            Directory luceneDirectory)
        {
            string[] keysToDeleteAfterRecovery = null;
            if (indexDefinition.IsMapReduce == false)
            {
                IndexCommitPoint commitUsedToRestore;

                if (TryReusePreviousCommitPointsToRecoverIndex(luceneDirectory,
                                                               indexDefinition, path,
                                                               out commitUsedToRestore,
                                                               out keysToDeleteAfterRecovery))
                {
                    ResetLastIndexedEtag(indexDefinition, commitUsedToRestore.HighestCommitedETag, commitUsedToRestore.TimeStamp);
                }
            }
            else
            {
                RegenerateMapReduceIndex(luceneDirectory, indexDefinition);
            }
            return keysToDeleteAfterRecovery;
        }

        private void LoadExistingSuggestionsExtentions(string indexName, Index indexImplementation)
        {
            var suggestionsForIndex = Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName);
            if (!System.IO.Directory.Exists(suggestionsForIndex))
                return;

            try
            {
                var directories = System.IO.Directory.GetDirectories(suggestionsForIndex);
                if (directories.Any(dir => dir.Contains("-")))
                {
                    // Legacy handling:
                    // Previously we had separate folder with suggestions for each triple: (field, distanceType, accuracy)
                    // Now we have field only.
                    // Legacy naming convention was: field-{distanceType}-{accuracy}
                    // since when we have - (dash) in SOME folder name it seems to be legacy
                    HandleLegacySuggestions(directories);

                    // Refresh directories list as handling legacy might rename or delete some of them.					
                    directories = System.IO.Directory.GetDirectories(suggestionsForIndex);
                }

                foreach (var directory in directories)
                {
                    IndexSearcher searcher;
                    using (indexImplementation.GetSearcher(out searcher))
                    {
                        var key = Path.GetFileName(directory);
                        var field = MonoHttpUtility.UrlDecode(key);
                        var extension = new SuggestionQueryIndexExtension(
                            indexImplementation,
                            documentDatabase.WorkContext,
                            Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName, key),
                            searcher.IndexReader.Directory() is RAMDirectory,
                            field);
                        indexImplementation.SetExtension(key, extension);
                    }
                }
            }
            catch (Exception e)
            {
                log.WarnException("Could not open suggestions for index " + indexName + ", resetting the index", e);
                try
                {
                    IOExtensions.DeleteDirectory(suggestionsForIndex);
                }
                catch (Exception)
                {
                    // ignore the failure
                }
                throw;
            }
        }

        internal static void HandleLegacySuggestions(string[] directories)
        {
            var alreadySeenFields = new HashSet<string>();
            foreach (var directory in directories)
            {
                var key = Path.GetFileName(directory);
                var parentDir = System.IO.Directory.GetParent(directory).FullName;

                if (key.Contains("-"))
                {
                    var tokens = key.Split('-');
                    var field = tokens[0];
                    if (alreadySeenFields.Contains(field))
                    {
                        log.Info("Removing legacy suggestions: {0}", directory);
                        IOExtensions.DeleteDirectory(directory);
                    }
                    else
                    {
                        alreadySeenFields.Add(field);
                        var newLocation = Path.Combine(parentDir, field);

                        log.Info("Moving suggestions from: {0} to {1}", directory, newLocation);
                        System.IO.Directory.Move(directory, newLocation);
                    }
                }
                else
                {
                    alreadySeenFields.Add(key);
                }
            }
        }

        protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(IndexDefinition indexDefinition, bool createIfMissing = true, bool forceFullExistingIndexCheck = false)
        {
            Lucene.Net.Store.Directory directory;
            if (configuration.RunInMemory ||
                (indexDefinition.IsMapReduce == false &&  // there is no point in creating map/reduce indexes in memory, we write the intermediate results to disk anyway
                 indexDefinitionStorage.IsNewThisSession(indexDefinition) &&
                 indexDefinition.DisableInMemoryIndexing == false &&
                 configuration.DisableInMemoryIndexing == false &&
                 forceFullExistingIndexCheck == false))
            {
                directory = new RAMDirectory();
                new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose(); // creating index structure
            }
            else
            {
                var indexDirectory = indexDefinition.IndexId.ToString();
                var indexFullPath = Path.Combine(path, indexDirectory);
                directory = new LuceneCodecDirectory(indexFullPath, documentDatabase.IndexCodecs.OfType<AbstractIndexCodec>());

                if (!IndexReader.IndexExists(directory))
                {
                    if (createIfMissing == false)
                        throw new InvalidOperationException(string.Format("Index directory '{0}' does not exists for '{1}' index.", indexFullPath, indexDefinition.Name));

                    WriteIndexVersion(directory, indexDefinition);

                    //creating index structure if we need to
                    new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
                }
                else
                {
                    EnsureIndexVersionMatches(directory, indexDefinition);

                    if (forceFullExistingIndexCheck == false)
                    {
                        if (directory.FileExists("write.lock")) // force lock release, because it was still open when we shut down
                        {
                            IndexWriter.Unlock(directory);
                            // for some reason, just calling unlock doesn't remove this file
                            directory.DeleteFile("write.lock");
                        }
                        if (directory.FileExists("writing-to-index.lock")) // we had an unclean shutdown
                        {
                            if (configuration.ResetIndexOnUncleanShutdown)
                                throw new InvalidOperationException(string.Format("Rude shutdown detected on '{0}' index in '{1}' directory.", indexDefinition.Name, indexFullPath));

                            CheckIndexAndTryToFix(directory, indexDefinition);
                            directory.DeleteFile("writing-to-index.lock");
                        }
                    }
                    else
                    {
                        IndexWriter.Unlock(directory);

                        if (directory.FileExists("write.lock"))
                            directory.DeleteFile("write.lock");

                        CheckIndexAndTryToFix(directory, indexDefinition);

                        if (directory.FileExists("writing-to-index.lock"))
                            directory.DeleteFile("writing-to-index.lock");
                    }
                }
            }

            return directory;

        }

        private void RegenerateMapReduceIndex(Directory directory, IndexDefinition indexDefinition)
        {
            // remove old index data
            var dirOnDisk = Path.Combine(path, indexDefinition.IndexId.ToString());
            IOExtensions.DeleteDirectory(dirOnDisk);

            // initialize by new index
            System.IO.Directory.CreateDirectory(dirOnDisk);
            WriteIndexVersion(directory, indexDefinition);
            new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();

            var start = 0;
            const int take = 100;

            documentDatabase.TransactionalStorage.Batch(actions =>
            {
                IList<ReduceTypePerKey> reduceKeysAndTypes;

                do
                {
                    reduceKeysAndTypes = actions.MapReduce.GetReduceKeysAndTypes(indexDefinition.IndexId, start, take).ToList();
                    start += take;

                    var keysToScheduleOnLevel2 =
                        reduceKeysAndTypes.Where(x => x.OperationTypeToPerform == ReduceType.MultiStep).ToList();
                    var keysToScheduleOnLevel0 =
                        reduceKeysAndTypes.Where(x => x.OperationTypeToPerform == ReduceType.SingleStep).ToList();

                    var itemsToScheduleOnLevel2 = keysToScheduleOnLevel2.Select(x => new ReduceKeyAndBucket(0, x.ReduceKey)).ToList();
                    var itemsToScheduleOnLevel0 = new List<ReduceKeyAndBucket>();

                    foreach (var reduceKey in keysToScheduleOnLevel0.Select(x => x.ReduceKey))
                    {
                        var mappedBuckets = actions.MapReduce.GetMappedBuckets(indexDefinition.IndexId, reduceKey, CancellationToken.None).Distinct();

                        itemsToScheduleOnLevel0.AddRange(mappedBuckets.Select(x => new ReduceKeyAndBucket(x, reduceKey)));
                    }

                    foreach (var itemToReduce in itemsToScheduleOnLevel2)
                    {
                        actions.MapReduce.ScheduleReductions(indexDefinition.IndexId, 2, itemToReduce);
                        actions.General.MaybePulseTransaction();
                    }

                    foreach (var itemToReduce in itemsToScheduleOnLevel0)
                    {
                        actions.MapReduce.ScheduleReductions(indexDefinition.IndexId, 0, itemToReduce);
                        actions.General.MaybePulseTransaction();
                    }

                } while (reduceKeysAndTypes.Count > 0);
            });
        }

        private void ResetLastIndexedEtag(IndexDefinition indexDefinition, Etag lastIndexedEtag, DateTime timestamp)
        {
            documentDatabase.TransactionalStorage.Batch(
                accessor =>
                accessor.Indexing.UpdateLastIndexed(indexDefinition.IndexId, lastIndexedEtag, timestamp));
        }

        internal Etag GetLastEtagForIndex(Index index)
        {
            if (index.IsMapReduce)
                return null;

            IndexStats stats = null;
            documentDatabase.TransactionalStorage.Batch(accessor => stats = accessor.Indexing.GetIndexStats(index.IndexId));

            return stats != null ? stats.LastIndexedEtag : Etag.Empty;
        }

        public static string IndexVersionFileName(IndexDefinition indexDefinition)
        {
            if (indexDefinition.IsMapReduce)
                return "mapReduce.version";
            return "index.version";
        }

        public static void WriteIndexVersion(Directory directory, IndexDefinition indexDefinition)
        {
            var version = IndexVersion;
            if (indexDefinition.IsMapReduce)
            {
                version = MapReduceIndexVersion;
            }
            using (var indexOutput = directory.CreateOutput(IndexVersionFileName(indexDefinition)))
            {
                indexOutput.WriteString(version);
                indexOutput.Flush();
            }
        }

        private static void EnsureIndexVersionMatches(Directory directory, IndexDefinition indexDefinition)
        {
            var versionToCheck = IndexVersion;
            if (indexDefinition.IsMapReduce)
            {
                versionToCheck = MapReduceIndexVersion;
            }
            var indexVersion = IndexVersionFileName(indexDefinition);
            if (directory.FileExists(indexVersion) == false)
            {
                throw new InvalidOperationException("Could not find " + indexVersion + " " + indexDefinition.IndexId + ", resetting index");
            }
            using (var indexInput = directory.OpenInput(indexVersion))
            {
                var versionFromDisk = indexInput.ReadString();
                if (versionFromDisk != versionToCheck)
                    throw new InvalidOperationException("Index " + indexDefinition.IndexId + " is of version " + versionFromDisk +
                                                        " which is not compatible with " + versionToCheck + ", resetting index");
            }
        }

        private static void CheckIndexAndTryToFix(Directory directory, IndexDefinition indexDefinition)
        {
            startupLog.Warn("Unclean shutdown detected on {0}, checking the index for errors. This may take a while.", indexDefinition.Name);

            var memoryStream = new MemoryStream();
            var stringWriter = new StreamWriter(memoryStream);
            var checkIndex = new CheckIndex(directory);

            if (startupLog.IsWarnEnabled)
                checkIndex.SetInfoStream(stringWriter);

            var sp = Stopwatch.StartNew();
            var status = checkIndex.CheckIndex_Renamed_Method();
            sp.Stop();
            if (startupLog.IsWarnEnabled)
            {
                startupLog.Warn("Checking index {0} took: {1}, clean: {2}", indexDefinition.Name, sp.Elapsed, status.clean);
                memoryStream.Position = 0;

                log.Warn(new StreamReader(memoryStream).ReadToEnd());
            }

            if (status.clean)
                return;

            startupLog.Warn("Attempting to fix index: {0}", indexDefinition.Name);
            sp.Restart();
            checkIndex.FixIndex(status);
            startupLog.Warn("Fixed index {0} in {1}", indexDefinition.Name, sp.Elapsed);
        }

        public void StoreCommitPoint(string indexName, IndexCommitPoint indexCommit)
        {
            if (indexCommit.SegmentsInfo == null || indexCommit.SegmentsInfo.IsIndexCorrupted)
                return;

            var directoryName = indexCommit.SegmentsInfo.Generation.ToString("0000000000000000000", CultureInfo.InvariantCulture);
            var commitPointDirectory = new IndexCommitPointDirectory(path, indexName, directoryName);

            if (System.IO.Directory.Exists(commitPointDirectory.AllCommitPointsFullPath) == false)
            {
                System.IO.Directory.CreateDirectory(commitPointDirectory.AllCommitPointsFullPath);
            }

            System.IO.Directory.CreateDirectory(commitPointDirectory.FullPath);

            using (var commitPointFile = File.Create(commitPointDirectory.FileFullPath))
            using (var sw = new StreamWriter(commitPointFile))
            {
                var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
                var textWriter = new JsonTextWriter(sw);

                jsonSerializer.Serialize(textWriter, indexCommit);
                sw.Flush();
            }

            var currentSegmentsFileName = indexCommit.SegmentsInfo.SegmentsFileName;

            File.Copy(Path.Combine(commitPointDirectory.IndexFullPath, currentSegmentsFileName),
                        Path.Combine(commitPointDirectory.FullPath, currentSegmentsFileName),
                        overwrite: true);

            var storedCommitPoints = System.IO.Directory.GetDirectories(commitPointDirectory.AllCommitPointsFullPath);

            if (storedCommitPoints.Length > configuration.MaxNumberOfStoredCommitPoints)
            {
                foreach (var toDelete in storedCommitPoints.Take(storedCommitPoints.Length - configuration.MaxNumberOfStoredCommitPoints))
                {
                    IOExtensions.DeleteDirectory(toDelete);
                }
            }
        }

        public void AddDeletedKeysToCommitPoints(IndexDefinition indexDefinition, string[] deletedKeys)
        {
            var indexFullPath = Path.Combine(path, indexDefinition.IndexId.ToString());

            var existingCommitPoints = IndexCommitPointDirectory.ScanAllCommitPointsDirectory(indexFullPath);

            foreach (var commitPointDirectory in existingCommitPoints.Select(commitPoint => new IndexCommitPointDirectory(path, indexDefinition.IndexId.ToString(), commitPoint)))
            {
                using (var stream = File.Open(commitPointDirectory.DeletedKeysFile, FileMode.OpenOrCreate))
                {
                    stream.Seek(0, SeekOrigin.End);
                    using (var writer = new StreamWriter(stream))
                    {
                        foreach (var deletedKey in deletedKeys)
                        {
                            writer.WriteLine(deletedKey);
                        }
                    }
                }
            }
        }

        private bool TryReusePreviousCommitPointsToRecoverIndex(Directory directory, IndexDefinition indexDefinition, string indexStoragePath, out IndexCommitPoint indexCommit, out string[] keysToDelete)
        {
            indexCommit = null;
            keysToDelete = null;

            if (indexDefinition.IsMapReduce)
                return false;

            var indexFullPath = Path.Combine(indexStoragePath, indexDefinition.IndexId.ToString());



            var allCommitPointsFullPath = IndexCommitPointDirectory.GetAllCommitPointsFullPath(indexFullPath);

            if (System.IO.Directory.Exists(allCommitPointsFullPath) == false)
                return false;

            var filesInIndexDirectory = System.IO.Directory.GetFiles(indexFullPath).Select(Path.GetFileName);

            var existingCommitPoints =
                IndexCommitPointDirectory.ScanAllCommitPointsDirectory(indexFullPath);

            Array.Reverse(existingCommitPoints); // start from the highest generation

            foreach (var commitPointDirectoryName in existingCommitPoints)
            {
                try
                {
                    var commitPointDirectory = new IndexCommitPointDirectory(indexStoragePath, indexDefinition.IndexId.ToString(),
                                                                                commitPointDirectoryName);

                    if (TryGetCommitPoint(commitPointDirectory, out indexCommit) == false)
                    {
                        IOExtensions.DeleteDirectory(commitPointDirectory.FullPath);
                        continue; // checksum is invalid, try another commit point
                    }

                    var missingFile =
                        indexCommit.SegmentsInfo.ReferencedFiles.Any(
                            referencedFile => filesInIndexDirectory.Contains(referencedFile) == false);

                    if (missingFile)
                    {
                        IOExtensions.DeleteDirectory(commitPointDirectory.FullPath);
                        continue; // there are some missing files, try another commit point
                    }

                    var storedSegmentsFile = indexCommit.SegmentsInfo.SegmentsFileName;

                    // here there should be only one segments_N file, however remove all if there is more
                    foreach (var currentSegmentsFile in System.IO.Directory.GetFiles(commitPointDirectory.IndexFullPath, "segments_*"))
                    {
                        File.Delete(currentSegmentsFile);
                    }

                    // copy old segments_N file
                    File.Copy(Path.Combine(commitPointDirectory.FullPath, storedSegmentsFile),
                              Path.Combine(commitPointDirectory.IndexFullPath, storedSegmentsFile), true);

                    try
                    {
                        // update segments.gen file
                        using (var genOutput = directory.CreateOutput(IndexFileNames.SEGMENTS_GEN))
                        {
                            genOutput.WriteInt(SegmentInfos.FORMAT_LOCKLESS);
                            genOutput.WriteLong(indexCommit.SegmentsInfo.Generation);
                            genOutput.WriteLong(indexCommit.SegmentsInfo.Generation);
                        }
                    }
                    catch (Exception)
                    {
                        // here we can ignore, segments.gen is used only as fallback
                    }

                    if (File.Exists(commitPointDirectory.DeletedKeysFile))
                        keysToDelete = File.ReadLines(commitPointDirectory.DeletedKeysFile).ToArray();

                    return true;
                }
                catch (Exception ex)
                {
                    startupLog.WarnException("Could not recover an index named '" + indexDefinition.IndexId +
                                       "'from segments of the following generation " + commitPointDirectoryName, ex);
                }
            }

            return false;
        }

        public static IndexSegmentsInfo GetCurrentSegmentsInfo(string indexName, Directory directory)
        {
            var segmentInfos = new SegmentInfos();
            var result = new IndexSegmentsInfo();

            try
            {
                segmentInfos.Read(directory);

                result.Generation = segmentInfos.Generation;
                result.SegmentsFileName = segmentInfos.GetCurrentSegmentFileName();
                result.ReferencedFiles = segmentInfos.Files(directory, false);
            }
            catch (CorruptIndexException ex)
            {
                log.WarnException(string.Format("Could not read segment information for an index '{0}'", indexName), ex);

                result.IsIndexCorrupted = true;
            }

            return result;
        }

        public static bool TryGetCommitPoint(IndexCommitPointDirectory commitPointDirectory, out IndexCommitPoint indexCommit)
        {
            using (var commitPointFile = File.OpenRead(commitPointDirectory.FileFullPath))
            {
                try
                {
                    var textReader = new JsonTextReader(new StreamReader(commitPointFile));
                    var jsonCommitPoint = RavenJObject.Load(textReader);
                    var jsonEtag = jsonCommitPoint.Value<RavenJToken>("HighestCommitedETag");

                    Etag recoveredEtag = null;
                    if (jsonEtag.Type == JTokenType.Object) // backward compatibility - HighestCommitedETag is written as {"Restarts":123,"Changes":1}
                    {
                        jsonCommitPoint.Remove("HighestCommitedETag");
                        recoveredEtag = new Etag(UuidType.Documents, jsonEtag.Value<long>("Restarts"), jsonEtag.Value<long>("Changes"));
                    }

                    indexCommit = jsonCommitPoint.JsonDeserialization<IndexCommitPoint>();

                    if (indexCommit == null)
                        return false;

                    if (recoveredEtag != null)
                        indexCommit.HighestCommitedETag = recoveredEtag;

                    if (indexCommit.HighestCommitedETag == null || indexCommit.HighestCommitedETag.CompareTo(Etag.Empty) == 0)
                        return false;

                    return true;
                }
                catch (Exception e)
                {
                    log.Warn("Could not get commit point from the following location {0}. Exception {1}", commitPointDirectory.FileFullPath, e);

                    indexCommit = null;
                    return false;
                }
            }
        }

        internal Directory MakeRAMDirectoryPhysical(RAMDirectory ramDir, IndexDefinition indexDefinition)
        {
            var newDir = new LuceneCodecDirectory(Path.Combine(path, indexDefinition.IndexId.ToString()), documentDatabase.IndexCodecs.OfType<AbstractIndexCodec>());
            Directory.Copy(ramDir, newDir, false);
            return newDir;
        }

        private Index CreateIndexImplementation(IndexDefinition indexDefinition, Directory directory)
        {
            var viewGenerator = indexDefinitionStorage.GetViewGenerator(indexDefinition.IndexId);
            var indexImplementation = indexDefinition.IsMapReduce
                                        ? (Index)new MapReduceIndex(directory, indexDefinition.IndexId, indexDefinition, viewGenerator, documentDatabase.WorkContext)
                                        : new SimpleIndex(directory, indexDefinition.IndexId, indexDefinition, viewGenerator, documentDatabase.WorkContext);

            configuration.Container.SatisfyImportsOnce(indexImplementation);

            indexImplementation.AnalyzerGenerators.Apply(initialization => initialization.Initialize(documentDatabase));

            return indexImplementation;
        }

        public int[] Indexes
        {
            get { return indexes.Keys.ToArray(); }
        }

        public string[] IndexNames
        {
            get { return indexes.Values.Select(x => x.PublicName).ToArray(); }
        }

        public bool HasIndex(string index)
        {
            if (index == null)
                return false;
            return indexes.Any(x => String.Compare(index, x.Value.PublicName, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(log, "Could not properly close index storage");

            exceptionAggregator.Execute(FlushMapIndexes);
            exceptionAggregator.Execute(FlushReduceIndexes);

            exceptionAggregator.Execute(() => Parallel.ForEach(indexes.Values, index => exceptionAggregator.Execute(index.Dispose)));

            exceptionAggregator.Execute(() => dummyAnalyzer.Close());

            exceptionAggregator.Execute(() =>
            {
                if (crashMarker != null)
                    crashMarker.Dispose();
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void DeleteIndex(string name)
        {
            var value = TryIndexByName(name);
            if (value == null)
                return;
            DeleteIndex(value.indexId);
        }

        public void DeleteIndex(int id)
        {
            var value = GetIndexInstance(id);
            if (value == null)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Ignoring delete for non existing index {0}", id);
                return;
            }
            documentDatabase.TransactionalStorage.Batch(accessor =>
              accessor.Lists.Remove("Raven/Indexes/QueryTime", value.PublicName));
            if (log.IsDebugEnabled)
                log.Debug("Deleting index {0}", value.PublicName);
            value.Dispose();
            Index ignored;

            var dirOnDisk = Path.Combine(path, id.ToString());
            if (!indexes.TryRemove(id, out ignored) || !System.IO.Directory.Exists(dirOnDisk))
                return;

            UpdateIndexMappingFile();
        }

        public void RenameIndex(IndexDefinition existingIndex, string newIndexName)
        {
            var indexId = existingIndex.IndexId;
            var value = GetIndexInstance(indexId);

            if (log.IsDebugEnabled)
                log.Debug("Renaming index {0} -> {1}", value.PublicName, newIndexName);


            // since all we have to do in storage layer is to update mapping file
            // we simply call UpdateIndexMappingFile
            // mapping was already updated in IndexDefinitionStorage.RenameIndex method.
            UpdateIndexMappingFile();
        }

        public void DeleteIndexData(int id)
        {
            var dirOnDisk = Path.Combine(path, id.ToString(CultureInfo.InvariantCulture));
            IOExtensions.DeleteDirectory(dirOnDisk);
        }

        public Index ReopenCorruptedIndex(Index index)
        {
            if (index.Priority != IndexingPriority.Error)
                throw new InvalidOperationException(string.Format("Index {0} isn't errored", index.PublicName));

            index.Dispose();

            var reopened = OpenIndex(index.PublicName, onStartup: false, forceFullIndexCheck: true);

            if (reopened == null)
                throw new InvalidOperationException("Reopened index cannot be null instance. Index name:" + index.PublicName);

            return indexes.AddOrUpdate(reopened.IndexId, n => reopened, (s, existigIndex) => reopened);
        }

        public void CreateIndexImplementation(IndexDefinition indexDefinition)
        {
            if (log.IsDebugEnabled)
                log.Debug("Creating index {0} with id {1}", indexDefinition.IndexId, indexDefinition.Name);

            IndexDefinitionStorage.ResolveAnalyzers(indexDefinition);

            if (TryIndexByName(indexDefinition.Name) != null)
            {
                throw new InvalidOperationException("Index " + indexDefinition.Name + " already exists");
            }

            var addedIndex = indexes.AddOrUpdate(indexDefinition.IndexId, n =>
        {
            var directory = OpenOrCreateLuceneDirectory(indexDefinition);
            return CreateIndexImplementation(indexDefinition, directory);
        }, (s, index) => index);

            //prevent corrupted index when creating a map-reduce index
            //need to do this for every map reduce index, even when indexing is enabled,
            if (addedIndex.IsMapReduce)
            {
                addedIndex.EnsureIndexWriter();
                addedIndex.Flush(Etag.Empty);
            }

            UpdateIndexMappingFile();
        }

        public Query GetDocumentQuery(string index, IndexQuery query, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
        {
            var value = TryIndexByName(index);
            if (value == null)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Query on non existing index {0}", index);
                throw new InvalidOperationException("Index '" + index + "' does not exists");
            }
            var fieldsToFetch = new FieldsToFetch(new string[0], false, null);
            return new Index.IndexQueryOperation(value, query, _ => false, fieldsToFetch, indexQueryTriggers).GetDocumentQuery();
        }

        private Index TryIndexByName(string name)
        {
            return indexes.Where(index => String.Compare(index.Value.PublicName, name, StringComparison.OrdinalIgnoreCase) == 0)
                .Select(x => x.Value)
                .FirstOrDefault();
        }

        public IEnumerable<IndexQueryResult> Query(string index,
            IndexQuery query,
            Func<IndexQueryResult, bool> shouldIncludeInResults,
            FieldsToFetch fieldsToFetch,
            OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers,
            CancellationToken token,
            Action<double> parseTiming = null
            )
        {
            Index value = TryIndexByName(index);
            if (value == null)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Query on non existing index '{0}'", index);
                throw new InvalidOperationException("Index '" + index + "' does not exists");
            }

            if ((value.Priority.HasFlag(IndexingPriority.Idle) || value.Priority.HasFlag(IndexingPriority.Abandoned)) &&
                value.Priority.HasFlag(IndexingPriority.Forced) == false)
            {
                value.Priority = IndexingPriority.Normal;
                try
                {
                    documentDatabase.TransactionalStorage.Batch(accessor =>
                    {
                        accessor.Indexing.SetIndexPriority(value.indexId, IndexingPriority.Normal);
                    });
                }
                catch (ConcurrencyException)
                {
                    //we explicitly ignore write conflicts here, 
                    //it is okay if we got set twice (two concurrent queries, or setting while indexing).
                }

                documentDatabase.WorkContext.ShouldNotifyAboutWork(() => "Idle index queried");
                documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                {
                    Name = value.PublicName,
                    Type = IndexChangeTypes.IndexPromotedFromIdle
                });
            }

            var indexQueryOperation = new Index.IndexQueryOperation(value, query, shouldIncludeInResults, fieldsToFetch, indexQueryTriggers);

            if (parseTiming != null)
                parseTiming(indexQueryOperation.QueryParseDuration.TotalMilliseconds);

            if (query.Query != null && query.Query.Contains(Constants.IntersectSeparator))
                return indexQueryOperation.IntersectionQuery(token);

            return indexQueryOperation.Query(token);
        }

        public IEnumerable<RavenJObject> IndexEntires(
            string indexName,
            IndexQuery query,
            List<string> reduceKeys,
            OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers,
            Reference<int> totalResults)
        {
            Index value = TryIndexByName(indexName);
            if (value == null)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Query on non existing index '{0}'", indexName);

                throw new InvalidOperationException("Index '" + indexName + "' does not exists");
            }

            var indexQueryOperation = new Index.IndexQueryOperation(value, query, null, new FieldsToFetch(null, false, null), indexQueryTriggers, reduceKeys);
            return indexQueryOperation.IndexEntries(totalResults);
        }

        public void RemoveFromIndex(int index, string[] keys, WorkContext context)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Removing from non existing index '{0}', ignoring", index);

                return;
            }

            value.Remove(keys, context);
            context.RaiseIndexChangeNotification(new IndexChangeNotification
            {
                Name = value.PublicName,
                Type = IndexChangeTypes.RemoveFromIndex
            });
        }

        [CLSCompliant(false)]
        public IndexingPerformanceStats Index(int index, AbstractViewGenerator viewGenerator, IndexingBatch batch, WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp, CancellationToken token)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Tried to index on a non existent index {0}, ignoring", index);
                return null;
            }
            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentCacher.SkipSetAndGetDocumentsInDocumentCache())
            {
                var performance = value.IndexDocuments(viewGenerator, batch, actions, minimumTimestamp, token);
                context.RaiseIndexChangeNotification(new IndexChangeNotification
                {
                    Name = value.PublicName,
                    Type = IndexChangeTypes.MapCompleted,
                    Collections = batch.Collections
                });

                return performance;
            }
        }

        [CLSCompliant(false)]
        public IndexingPerformanceStats Reduce(
            int index,
            AbstractViewGenerator viewGenerator,
            IEnumerable<IGrouping<int, object>> mappedResults,
            int level,
            WorkContext context,
            IStorageActionsAccessor actions,
            HashSet<string> reduceKeys,
            int inputCount)
        {
            Index value;
            if (indexes.TryGetValue(index, out value) == false)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Tried to index on a non existent index {0}, ignoring", index);
                return null;
            }

            var mapReduceIndex = value as MapReduceIndex;
            if (mapReduceIndex == null)
            {
                log.Warn("Tried to reduce on an index that is not a map/reduce index: {0}, ignoring", index);
                return null;
            }

            using (CultureHelper.EnsureInvariantCulture())
            {
                var reduceDocuments = new MapReduceIndex.ReduceDocuments(mapReduceIndex, viewGenerator, mappedResults, level, context, actions, reduceKeys, inputCount);

                var performance = reduceDocuments.ExecuteReduction();

                context.RaiseIndexChangeNotification(new IndexChangeNotification
                {
                    Name = value.PublicName,
                    Type = IndexChangeTypes.ReduceCompleted
                });

                return performance;
            }
        }

        internal IndexSearcherHolder.IndexSearcherHoldingState GetCurrentStateHolder(string indexName)
        {
            return GetIndexByName(indexName).GetCurrentStateHolder();
        }

        public IDisposable GetCurrentIndexSearcher(int indexId, out IndexSearcher searcher)
        {
            return GetIndexInstance(indexId).GetSearcher(out searcher);
        }

        public IDisposable GetCurrentIndexSearcherAndTermDocs(string indexName, out IndexSearcher searcher, out RavenJObject[] termsDocs)
        {
            return GetIndexByName(indexName).GetSearcherAndTermsDocs(out searcher, out termsDocs);
        }

        private Index GetIndexByName(string indexName)
        {
            var result = TryIndexByName(indexName);
            if (result == null)
                throw new InvalidOperationException(string.Format("Index '{0}' does not exist", indexName));
            return result;
        }

        public void RunIdleOperations()
        {
            foreach (var value in indexes.Values)
            {
                if ((SystemTime.UtcNow - value.LastIndexTime).TotalMinutes < 1)
                    continue;

                value.Flush(value.GetLastEtagFromStats());
            }

            SetUnusedIndexesToIdle();
            UpdateLatestPersistedQueryTime();
            DeleteSurpassedAutoIndexes();
        }

        public bool IsIndexStale(string indexName, LastCollectionEtags lastCollectionEtags)
        {
            var index = TryIndexByName(indexName);

            if (index == null)
                throw new InvalidOperationException("Could not find index " + indexName);

            return IsIndexStale(index.IndexId, lastCollectionEtags);
        }

        public bool IsIndexStale(int indexId, LastCollectionEtags lastCollectionEtags)
        {
            bool isStale = false;

            documentDatabase.TransactionalStorage.Batch(actions =>
            {
                Index indexInstance = GetIndexInstance(indexId);

                isStale = (indexInstance != null && indexInstance.IsMapIndexingInProgress) || actions.Staleness.IsIndexStale(indexId, null, null);

                if (indexInstance != null && indexInstance.IsTestIndex)
                    isStale = false;

                if (isStale && actions.Staleness.IsIndexStaleByTask(indexId, null) == false && actions.Staleness.IsReduceStale(indexId) == false)
                {
                    var viewGenerator = indexDefinitionStorage.GetViewGenerator(indexId);
                    if (viewGenerator == null)
                        return;

                    var indexStats = actions.Indexing.GetIndexStats(indexId);
                    if (indexStats == null)
                        return;

                    var lastIndexedEtag = indexStats.LastIndexedEtag;
                    var collectionNames = viewGenerator.ForEntityNames.ToList();
                    if (lastCollectionEtags.HasEtagGreaterThan(collectionNames, lastIndexedEtag) == false)
                        isStale = false;
                }
            });

            return isStale;
        }

        private void DeleteSurpassedAutoIndexes()
        {
            if (indexes.Any(x => x.Value.PublicName.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase)) == false)
                return;

            var mergeSuggestions = indexDefinitionStorage.ProposeIndexMergeSuggestions();

            foreach (var mergeSuggestion in mergeSuggestions.Suggestions)
            {
                if (string.IsNullOrEmpty(mergeSuggestion.SurpassingIndex))
                    continue;

                if (mergeSuggestion.CanDelete.Any(x => x.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase)) == false)
                    continue;

                if (IsIndexStale(mergeSuggestion.SurpassingIndex, documentDatabase.LastCollectionEtags))
                    continue;

                foreach (var indexToDelete in mergeSuggestion.CanDelete.Where(x => x.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase)))
                {
                    documentDatabase.Indexes.DeleteIndex(indexToDelete);
                }
            }
        }

        private void UpdateLatestPersistedQueryTime()
        {
            documentDatabase.TransactionalStorage.Batch(accessor =>
            {
                var maxDate = latestPersistedQueryTime;
                foreach (var index in indexes)
                {
                    var lastQueryTime = index.Value.LastQueryTime ?? DateTime.MinValue;
                    if (lastQueryTime <= latestPersistedQueryTime)
                        continue;

                    accessor.Lists.Set("Raven/Indexes/QueryTime", index.Value.PublicName, new RavenJObject
                    {
                        {"LastQueryTime", lastQueryTime}
                    }, UuidType.Indexing);

                    if (lastQueryTime > maxDate)
                        maxDate = lastQueryTime;
                }
                latestPersistedQueryTime = maxDate;
            });
        }

        public class UnusedIndexState
        {
            public DateTime LastQueryTime { get; set; }
            public Index Index { get; set; }
            public string Name { get; set; }
            public IndexingPriority Priority { get; set; }
            public DateTime CreationDate { get; set; }
        }

        private void SetUnusedIndexesToIdle()
        {
            documentDatabase.TransactionalStorage.Batch(accessor =>
            {
                var autoIndexesSortedByLastQueryTime =
                    (from index in indexes
                     let stats = GetIndexStats(accessor, index.Key)
                     where stats != null
                     let lastQueryTime = stats.LastQueryTimestamp ?? DateTime.MinValue
                     where index.Value.PublicName.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase)
                     orderby lastQueryTime
                     select new UnusedIndexState
                     {
                         LastQueryTime = lastQueryTime,
                         Index = index.Value,
                         Name = index.Value.PublicName,
                         Priority = stats.Priority,
                         CreationDate = stats.CreatedTimestamp
                     }).ToArray();

                var timeToWaitBeforeMarkingAutoIndexAsIdle = documentDatabase.Configuration.TimeToWaitBeforeMarkingAutoIndexAsIdle;
                var timeToWaitForIdleMinutes = timeToWaitBeforeMarkingAutoIndexAsIdle.TotalMinutes * 10;

                for (var i = 0; i < autoIndexesSortedByLastQueryTime.Length; i++)
                {
                    var thisItem = autoIndexesSortedByLastQueryTime[i];

                    if (thisItem.Priority.HasFlag(IndexingPriority.Disabled) || // we don't really have much to say about those in here
                        thisItem.Priority.HasFlag(IndexingPriority.Error) || // no need to touch erroring indexes
                        thisItem.Priority.HasFlag(IndexingPriority.Forced))// if it is forced, we can't change it
                        continue;

                    var age = (SystemTime.UtcNow - thisItem.CreationDate).TotalMinutes;
                    var lastQuery = (SystemTime.UtcNow - thisItem.LastQueryTime).TotalMinutes;

                    if (thisItem.Priority.HasFlag(IndexingPriority.Normal))
                    {
                        if (age < timeToWaitForIdleMinutes)
                        {
                            HandleActiveIndex(thisItem, age, lastQuery, accessor, timeToWaitForIdleMinutes);
                        }
                        else
                        {
                            // If it's a fairly established query then we need to determine whether there is any activity currently
                            // If there is activity and this has not been queried against 'recently' it needs idling
                            if (i < autoIndexesSortedByLastQueryTime.Length - 1)
                            {
                                var nextItem = autoIndexesSortedByLastQueryTime[i + 1];
                                if ((nextItem.LastQueryTime - thisItem.LastQueryTime).TotalMinutes > timeToWaitForIdleMinutes)
                                {
                                    accessor.Indexing.SetIndexPriority(thisItem.Index.indexId, IndexingPriority.Idle);
                                    thisItem.Index.Priority = IndexingPriority.Idle;
                                    documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification()
                                    {
                                        Name = thisItem.Name,
                                        Type = IndexChangeTypes.IndexDemotedToIdle
                                    });
                                }
                            }
                        }

                        continue;
                    }

                    if (thisItem.Priority.HasFlag(IndexingPriority.Idle))
                    {
                        HandleIdleIndex(age, lastQuery, thisItem, accessor);
                        continue;
                    }
                }
            });
        }

        private IndexStats GetIndexStats(IStorageActionsAccessor accessor, int indexId)
        {
            var indexStats = accessor.Indexing.GetIndexStats(indexId);
            if (indexStats == null)
                return null;
            indexStats.LastQueryTimestamp = GetLastQueryTime(indexId);
            return indexStats;
        }


        private void HandleIdleIndex(double age, double lastQuery, UnusedIndexState thisItem,
                                            IStorageActionsAccessor accessor)
        {
            // relatively young index, haven't been queried for a while already
            // can be safely removed, probably
            if (age < 90 && lastQuery > 30)
            {
                accessor.Indexing.DeleteIndex(thisItem.Index.indexId, documentDatabase.WorkContext.CancellationToken);
                return;
            }

            if (lastQuery < configuration.TimeToWaitBeforeMarkingIdleIndexAsAbandoned.TotalMinutes)
                return;

            // old enough, and haven't been queried for a while, mark it as abandoned

            accessor.Indexing.SetIndexPriority(thisItem.Index.indexId, IndexingPriority.Abandoned);

            thisItem.Index.Priority = IndexingPriority.Abandoned;

            documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification()
            {
                Name = thisItem.Name,
                Type = IndexChangeTypes.IndexDemotedToAbandoned
            });
        }

        private void HandleActiveIndex(UnusedIndexState thisItem, double age, double lastQuery, IStorageActionsAccessor accessor, double timeToWaitForIdle)
        {
            if (age < (timeToWaitForIdle * 2.5) && lastQuery < (1.5 * timeToWaitForIdle))
                return;

            if (age < (timeToWaitForIdle * 6) && lastQuery < (2.5 * timeToWaitForIdle))
                return;

            accessor.Indexing.SetIndexPriority(thisItem.Index.indexId, IndexingPriority.Idle);

            thisItem.Index.Priority = IndexingPriority.Idle;

            documentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification()
            {
                Name = thisItem.Name,
                Type = IndexChangeTypes.IndexDemotedToIdle
            });
        }

        private void UpdateIndexMappingFile()
        {
            if (configuration.RunInMemory)
                return;

            var sb = new StringBuilder();

            foreach (var index in indexes)
            {
                sb.Append(string.Format("{0} - {1}{2}", index.Value.IndexId, index.Value.PublicName, Environment.NewLine));
            }

            File.WriteAllText(Path.Combine(path, "indexes.txt"), sb.ToString());
        }

        public void FlushMapIndexes()
        {
            if (indexes == null)
                return;

            foreach (var index in indexes)
            {
                if (index.Value.IsMapReduce == false)
                    FlushIndex(index.Value);
            }
        }

        public void FlushReduceIndexes()
        {
            if (indexes == null)
                return;

            foreach (var index in indexes)
            {
                if (index.Value.IsMapReduce)
                    FlushIndex(index.Value);
            }
        }

        public void FlushIndexes(HashSet<int> indexIds, bool onlyAddIndexError)
        {
            if (indexes == null || indexIds.Count == 0)
                return;

            foreach (var indexId in indexIds)
            {
                FlushIndex(indexId, onlyAddIndexError);
            }
        }

        public void FlushIndex(int indexId, bool onlyAddIndexError)
        {
            Index value;
            if (indexes.TryGetValue(indexId, out value))
                FlushIndex(value, onlyAddIndexError);
        }

        private static void FlushIndex(Index value, bool onlyAddIndexError = false)
        {
            var sp = Stopwatch.StartNew();

            try
            {
                value.Flush(value.GetLastEtagFromStats());
            }
            catch (Exception e)
            {
                value.HandleWriteError(e);
                log.WarnException(string.Format("Failed to flush {0} index: {1} (id: {2})",
                    GetIndexType(value.IsMapReduce), value.PublicName, value.IndexId), e);

                if (onlyAddIndexError)
                {
                    value.AddIndexFailedFlushError(e);
                    return;
                }

                throw;
            }

            if (log.IsDebugEnabled)
            {
                log.Debug("Flushed {0} index: {1} (id: {2}), took {3}ms",
                    GetIndexType(value.IsMapReduce), value.PublicName, value.IndexId, sp.ElapsedMilliseconds);
            }
        }

        private static string GetIndexType(bool isMapReduce)
        {
            return isMapReduce ? "map-reduce" : "simple map";
        }

        public List<int> GetDisabledIndexIds()
        {
            var indexIds = new List<int>();

            foreach (var index in indexes)
            {
                if (index.Value.Priority.HasFlag(IndexingPriority.Disabled))
                    indexIds.Add(index.Key);
            }

            return indexIds;
        }

        public IIndexExtension GetIndexExtension(string index, string indexExtensionKey)
        {
            return GetIndexByName(index).GetExtension(indexExtensionKey);
        }

        public IIndexExtension GetIndexExtensionByPrefix(string index, string indexExtensionKeyPrefix)
        {
            return GetIndexByName(index).GetExtensionByPrefix(indexExtensionKeyPrefix);
        }

        public void SetIndexExtension(string indexName, string indexExtensionKey, IIndexExtension suggestionQueryIndexExtension)
        {
            GetIndexByName(indexName).SetExtension(indexExtensionKey, suggestionQueryIndexExtension);
        }

        public Index GetIndexInstance(string indexName)
        {
            return TryIndexByName(indexName);
        }

        public IEnumerable<Index> GetAllIndexes()
        {
            return indexes.Values;
        }

        public Index GetIndexInstance(int indexId)
        {
            Index value;
            indexes.TryGetValue(indexId, out value);
            return value;
        }

        public void MarkCachedQuery(string indexName)
        {
            GetIndexByName(indexName).MarkQueried();
        }

        internal void SetLastQueryTime(string indexName, DateTime lastQueryTime)
        {
            GetIndexByName(indexName).MarkQueried(lastQueryTime);
        }

        public DateTime? GetLastQueryTime(int index)
        {
            return GetIndexInstance(index).LastQueryTime;
        }

        public DateTime? GetLastQueryTime(string index)
        {
            return GetIndexInstance(index).LastQueryTime;
        }

        public IndexingPerformanceStats[] GetIndexingPerformance(int index)
        {
            return GetIndexInstance(index).GetIndexingPerformance();
        }

        public void Backup(string directory, string incrementalTag = null, Action<string, Exception, BackupStatus.BackupMessageSeverity> notifyCallback = null, CancellationToken token = default(CancellationToken))
        {
            Parallel.ForEach(indexes.Values, index =>
                index.Backup(directory, path, incrementalTag, notifyCallback, token));
        }

        public void MergeAllIndexes()
        {
            Parallel.ForEach(indexes.Values, index =>
                                             index.MergeSegments());
        }

        public string IndexOnRam(int id)
        {
            return GetIndexInstance(id).IsOnRam;
        }

        public void ForceWriteToDiskAndWriteInMemoryIndexToDiskIfNecessary(string indexName)
        {
            var index = GetIndexByName(indexName);
            index.ForceWriteToDisk();
            index.WriteInMemoryIndexToDiskIfNecessary(Etag.Empty);
        }

        internal bool TryReplaceIndex(string indexName, string indexToReplaceName)
        {
            var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexName);
            if (indexDefinition == null)
            {
                //the replacing index doesn't exist
                return true;
            }

            var indexToReplace = indexDefinitionStorage.GetIndexDefinition(indexToReplaceName);
            if (indexToReplace != null)
            {
                switch (indexToReplace.LockMode)
                {
                    case IndexLockMode.SideBySide:
                        //keep the SideBySide lock mode from the replaced index
                        indexDefinition.LockMode = IndexLockMode.SideBySide;
                        break;
                    case IndexLockMode.LockedIgnore:
                        //we ignore this and need to delete the replacing index
                        documentDatabase.IndexStorage.DeleteIndex(indexName);
                        log.Info("An attempt to replace an index with lock mode: LockedIgnore by a side by side index was detected");
                        return true;
                    case IndexLockMode.LockedError:
                        log.Info("An attempt to replace an index with lock mode: LockedError by a side by side index was detected");
                        throw new InvalidOperationException("An attempt to replace an index, locked with LockedError, by a side by side index was detected.");
                }
            }

            var success = indexDefinitionStorage.ReplaceIndex(indexName, indexToReplaceName, () =>
            {
                //replace the index errors with the new index errors
                int? indexToReplaceId = null;
                if (indexToReplace != null)
                    indexToReplaceId = indexToReplace.IndexId;
                documentDatabase.WorkContext.ReplaceIndexingErrors(indexToReplaceName,
                    indexToReplaceId, indexName, indexDefinition.IndexId);
            });

            if (success == false)
                return false;

            documentDatabase.Indexes.DeleteIndex(indexToReplace, removeByNameMapping: false, clearErrors: false, isSideBySideReplacement: true);

            return true;
        }
    }
}
