// -----------------------------------------------------------------------
//  <copyright file="IndexActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class IndexActions : ActionsBase
    {
        private volatile bool isPrecomputedBatchForNewIndexIsRunning;
        private readonly object precomputedLock = new object();

        public IndexActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        internal IndexDefinition[] Definitions
        {
            get { return Database.IndexDefinitionStorage.IndexDefinitions.Select(inx => inx.Value).ToArray(); }
        }

        public string[] GetIndexFields(string index)
        {
            var abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
            return abstractViewGenerator == null ? new string[0] : abstractViewGenerator.Fields;
        }

        public Etag GetIndexEtag(string indexName, Etag previousEtag, string resultTransformer = null)
        {
            Etag lastDocEtag = Etag.Empty;
            Etag lastIndexedEtag = null;
            Etag lastReducedEtag = null;
            bool isStale = false;
            int touchCount = 0;
            TransactionalStorage.Batch(accessor =>
            {
                var indexInstance = Database.IndexStorage.GetIndexInstance(indexName);
                if (indexInstance == null)
                    return;
                isStale = (indexInstance.IsMapIndexingInProgress) ||
                          accessor.Staleness.IsIndexStale(indexInstance.indexId, null, null);
                lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                var indexStats = accessor.Indexing.GetIndexStats(indexInstance.indexId);
                if (indexStats != null)
                {
                    lastReducedEtag = indexStats.LastReducedEtag;
                    lastIndexedEtag = indexStats.LastIndexedEtag;
                }
                touchCount = accessor.Staleness.GetIndexTouchCount(indexInstance.indexId);
            });


            var indexDefinition = GetIndexDefinition(indexName);
            if (indexDefinition == null)
                return Etag.Empty; // this ensures that we will get the normal reaction of IndexNotFound later on.

            var list = new List<byte>();
            list.AddRange(indexDefinition.GetIndexHash());
            list.AddRange(Encoding.Unicode.GetBytes(indexName));
            if (string.IsNullOrWhiteSpace(resultTransformer) == false)
            {
                var abstractTransformer = IndexDefinitionStorage.GetTransformer(resultTransformer);
                if (abstractTransformer == null)
                    throw new InvalidOperationException("The result transformer: " + resultTransformer + " was not found");
                list.AddRange(abstractTransformer.GetHashCodeBytes());
            }
            list.AddRange(lastDocEtag.ToByteArray());
            list.AddRange(BitConverter.GetBytes(touchCount));
            list.AddRange(BitConverter.GetBytes(isStale));
            if (lastReducedEtag != null)
            {
                list.AddRange(lastReducedEtag.ToByteArray());
            }
            if (lastIndexedEtag != null)
            {
                list.AddRange(lastIndexedEtag.ToByteArray());
            }
            list.AddRange(BitConverter.GetBytes(UuidGenerator.LastDocumentTransactionEtag));

            var indexEtag = Etag.Parse(Encryptor.Current.Hash.Compute16(list.ToArray()));

            if (previousEtag != null && previousEtag != indexEtag)
            {
                // the index changed between the time when we got it and the time 
                // we actually call this, we need to return something random so that
                // the next time we won't get 304

                return Etag.InvalidEtag;
            }

            return indexEtag;
        }

        internal int CheckReferenceBecauseOfDocumentUpdate(
            string key, 
            IStorageActionsAccessor actions, 
            IEnumerable<string> participatingIds = null)
        {
            Stopwatch sp = null;
            var count = 0;
            
            using (Database.DocumentLock.Lock())
            using (Database.TransactionalStorage.DisableBatchNesting())
            {
                // in external transaction number of references will be >= from current transaction references
                Database.TransactionalStorage.Batch(externalActions =>
                {
                    var referencingKeys = externalActions.Indexing.GetDocumentsReferencing(key);

                    foreach (var referencing in referencingKeys)
                    {
                        if (participatingIds != null && participatingIds.Contains(referencing))
                                continue;

                        Etag preTouchEtag = null;
                        Etag afterTouchEtag = null;
                        try
                        {
                            count++;
                            actions.Documents.TouchDocument(referencing, out preTouchEtag, out afterTouchEtag);

                            if (afterTouchEtag != null)
                            {
                                var docMetadata = actions.Documents.DocumentMetadataByKey(referencing);

                                if (docMetadata != null)
                                {
                                    var entityName = docMetadata.Metadata.Value<string>(Constants.RavenEntityName);

                                    if (string.IsNullOrEmpty(entityName) == false)
                                        Database.LastCollectionEtags.Update(entityName, afterTouchEtag);
                                }
                            }
                        }
                        catch (ConcurrencyException)
                        {
                        }

                        if (preTouchEtag == null || afterTouchEtag == null)
                            continue;

                        if (actions.General.MaybePulseTransaction())
                        {
                            if (sp == null)
                                sp = Stopwatch.StartNew();
                            if (sp.Elapsed >= Database.Configuration.CheckReferenceBecauseOfDocumentUpdateTimeout)
                            {
                                throw new TimeoutException("Early failure when checking references for document '" + key + "', we waited over 30 seconds to touch all of the documents referenced by this document.\r\n" +
                                                           "The operation (and transaction) has been aborted, since to try longer (we already touched " + count + " documents) risk a thread abort.\r\n" +
                                                           "Consider restructuring your indexes to avoid LoadDocument on such a popular document.");
                            }
                        }
                    }
                });
            }

            if (sp != null)
                Log.Info("Updated references for {0}, count: {1}, took: {2}", key, count, sp.ElapsedMilliseconds);

            return count;
        }

        private static void IsIndexNameValid(string name)
        {
            var error = string.Format("Index name {0} not permitted. ", name).Replace("//", "__");

            if (name.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(error + "Index names starting with dynamic_ or dynamic/ are reserved!", "name");
            }

            if (name.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(error + "Index name dynamic is reserved!", "name");
            }

            if (name.Contains("//"))
            {
                throw new ArgumentException(error + "Index name cannot contain // (double slashes)", "name");
            }
        }

        public bool IndexHasChanged(string name, IndexDefinition definition)
        {
            if (name == null)
                throw new ArgumentNullException("name");

            name = name.Trim();
            IsIndexNameValid(name);

            var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);
            if (existingIndex == null)
                return true;

            var creationOption = FindIndexCreationOptions(definition, ref name);
            return creationOption == IndexCreationOptions.Create || creationOption == IndexCreationOptions.Update;
        }

        // only one index can be created at any given time
        // the method already handle attempts to create the same index, so we don't have to 
        // worry about this.
        [MethodImpl(MethodImplOptions.Synchronized)]
        public string PutIndex(string name, IndexDefinition definition, bool isReplication = false)
        {
            return PutIndexInternal(name, definition, isReplication: isReplication);
        }

        private string PutIndexInternal(string name, IndexDefinition definition, bool disableIndexBeforePut = false, 
            bool isUpdateBySideSide = false, IndexCreationOptions? creationOptions = null, bool isReplication = false)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            name = name.Trim();
            IsIndexNameValid(name);

            var deletedIndexVersion = IndexDefinitionStorage.GetDeletedIndexVersion(name, definition.IndexId);
            if (isReplication && definition.IndexVersion != null &&
                deletedIndexVersion > definition.IndexVersion)
            {
                // we got a new index definition from replication
                // where its version is smaller then the deleted one
                // probably from an outdated node
                return null;
            }

            var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);
            if (existingIndex != null)
            {
                var newIndexVersion = definition.IndexVersion;
                var currentIndexVersion = existingIndex.IndexVersion;

                // whether we update the index definition or not,
                // we need to update the index version
                existingIndex.IndexVersion = definition.IndexVersion =
                    Math.Max(currentIndexVersion ?? 0, newIndexVersion ?? 0);

                switch (isReplication)
                {
                    case true:
                        if (newIndexVersion != null && currentIndexVersion != null &&
                            newIndexVersion <= currentIndexVersion)
                        {
                            //this new index is an older version of the current one
                            return null;
                        }

                        // we need to update the lock mode only if it was updated by another server
                        existingIndex.LockMode = definition.LockMode;
                        break;
                    default:
                        if (CanUpdateIndex(name, definition, isUpdateBySideSide, existingIndex) == false)
                            return null;
                        break;
                }
            }

            AssertAnalyzersValid(definition);

            var existingIndexVersion = existingIndex?.IndexVersion;
            switch (creationOptions ?? FindIndexCreationOptions(definition, ref name))
            {
                case IndexCreationOptions.Noop:
                    definition.IndexVersion = Math.Max(definition.IndexVersion ?? 0, existingIndexVersion ?? 0);                    
                    return null;
                case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
                    // ensure that the code can compile
                    new DynamicViewCompiler(definition.Name, definition, Database.Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Database.Configuration).GenerateInstance();
                    IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(definition);

                    definition.IndexVersion = Math.Max(definition.IndexVersion ?? 0, existingIndexVersion ?? 0);
                    if (isReplication == false)
                        definition.IndexVersion++;
                    return null;
                case IndexCreationOptions.Update:
                    // ensure that the code can compile
                    new DynamicViewCompiler(definition.Name, definition, Database.Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Database.Configuration).GenerateInstance();
                    DeleteIndex(name);

                    definition.IndexVersion = Math.Max(definition.IndexVersion ?? 0, existingIndexVersion ?? 0);
                    if (isReplication == false)
                        definition.IndexVersion++;
                    break;
                case IndexCreationOptions.Create:
                    if (isReplication == false)
                    {
                        // we create a new index,
                        // we need to restore its previous IndexVersion (if it was deleted before)

                        var replacingIndexVersion = GetOriginalIndexVersion(definition.Name);
                        definition.IndexVersion = Math.Max(deletedIndexVersion, replacingIndexVersion);
                        definition.IndexVersion++;
                    }

                    break;
            }

            PutNewIndexIntoStorage(name, definition, disableIndexBeforePut);

            WorkContext.ClearErrorsFor(name);

            TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new IndexChangeNotification
            {
                Name = name,
                Type = IndexChangeTypes.IndexAdded,
                Version = definition.IndexVersion
            }));

            return name;
        }

        private bool CanUpdateIndex(string name, IndexDefinition definition, bool isUpdateBySideSide, IndexDefinition existingIndex)
        {
            switch (existingIndex.LockMode)
            {
                case IndexLockMode.SideBySide:
                    if (isUpdateBySideSide == false)
                    {
                        Log.Info("Index {0} not saved because it might be only updated by side-by-side index", name);
                        throw new InvalidOperationException("Can not overwrite locked index: " + name + ". This index can be only updated by side-by-side index.");
                    }

                    //keep the SideBySide lock mode from the replaced index
                    definition.LockMode = IndexLockMode.SideBySide;
                    break;
                case IndexLockMode.LockedIgnore:
                    Log.Info("Index {0} not saved because it was lock (with ignore)", name);
                    return false;
                case IndexLockMode.LockedError:
                    throw new InvalidOperationException("Can not overwrite locked index: " + name);
            }

            return true;
        }

        private int GetOriginalIndexVersion(string name)
        {
            if (name.StartsWith(Constants.SideBySideIndexNamePrefix) == false)
                return 0;

            var originalName = name.Substring(Constants.SideBySideIndexNamePrefix.Length);
            var existingIndex = IndexDefinitionStorage.GetIndexDefinition(originalName);
            if (existingIndex == null)
                return 0;

            return existingIndex.IndexVersion ?? 0;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public string[] PutIndexes(IndexToAdd[] indexesToAdd, bool isReplication)
        {
            var createdIndexes = new List<string>();
            var prioritiesList = new List<IndexingPriority>();
            try
            {
                foreach (var indexToAdd in indexesToAdd)
                {
                    var nameToAdd = PutIndexInternal(indexToAdd.Name, indexToAdd.Definition, disableIndexBeforePut: true, isReplication: isReplication);
                    if (nameToAdd == null)
                        continue;

                    createdIndexes.Add(nameToAdd);
                    prioritiesList.Add(indexToAdd.Priority);
                }

                var indexesIds = createdIndexes.Select(x => Database.IndexStorage.GetIndexInstance(x).indexId).ToArray();
                SetIndexesPriority(indexesIds, prioritiesList);

                for (var i = 0; i < createdIndexes.Count; i++)
                {
                    var indexName = createdIndexes[i];
                    var priority = prioritiesList[i];

                    var instance = Database.IndexStorage.GetIndexInstance(indexName);
                    if (instance == null)
                    {
                        Log.Warn("Couldn't set index priority because index named: '{0}' doesn't exist", indexName);
                        continue;
                    }

                    instance.Priority = priority;
                }

                return createdIndexes.ToArray();
            }
            catch (Exception e)
            {
                Log.WarnException("Could not create index batch", e);

                if (isReplication == false)
                {
                    foreach (var index in createdIndexes)
                    {
                        DeleteIndex(index);
                    }
                }

                throw;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<IndexInfo> PutSideBySideIndexes(SideBySideIndexes sideBySideIndexes, bool isReplication)
        {
            var createdIndexes = new List<IndexInfo>();
            var prioritiesList = new List<IndexingPriority>();
            try
            {
                foreach (var indexToAdd in sideBySideIndexes.IndexesToAdd)
                {
                    var originalIndexName = indexToAdd.Name.Trim();
                    var indexName = Constants.SideBySideIndexNamePrefix + originalIndexName;
                    var isSideBySide = true;

                    IndexCreationOptions? creationOptions = null;
                    //if there is no existing side by side index, we might need to update the old index
                    if (IndexDefinitionStorage.GetIndexDefinition(indexName) == null)
                    {
                        var originalIndexCreationOptions = FindIndexCreationOptions(indexToAdd.Definition, ref originalIndexName);
                        switch (originalIndexCreationOptions)
                        {
                            case IndexCreationOptions.Noop:
                                continue;
                            case IndexCreationOptions.Create:
                            case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
                                //cases in which we don't need to create a side by side index:
                                //1) index doesn't exist => need to create a new regular index
                                //2) there is an existing index and we need to update its definition without reindexing
                                indexName = originalIndexName;
                                isSideBySide = false;
                                creationOptions = originalIndexCreationOptions;
                                break;
                        }

                        //keep the SideBySide lock mode from the replaced index
                        indexToAdd.Definition.LockMode = GetCurrentLockMode(originalIndexName) ?? indexToAdd.Definition.LockMode;
                    }
                    var nameToAdd = PutIndexInternal(indexName, indexToAdd.Definition,
                        disableIndexBeforePut: true, isUpdateBySideSide: true, creationOptions: creationOptions, isReplication: isReplication);

                    if (nameToAdd == null)
                        continue;

                    createdIndexes.Add(new IndexInfo
                    {
                        Name = indexName,
                        OriginalName = originalIndexName,
                        IsSideBySide = isSideBySide,
                        MinimumEtagBeforeReplace = indexToAdd.MinimumEtagBeforeReplace ?? sideBySideIndexes.MinimumEtagBeforeReplace,
                        ReplaceTimeUtc = indexToAdd.ReplaceTimeUtc ?? sideBySideIndexes.ReplaceTimeUtc
                    });

                    prioritiesList.Add(indexToAdd.Priority);
                }

                var indexesIds = createdIndexes.Select(x => Database.IndexStorage.GetIndexInstance(x.Name).indexId).ToArray();
                SetIndexesPriority(indexesIds, prioritiesList);

                for (var i = 0; i < createdIndexes.Count; i++)
                {
                    var indexName = createdIndexes[i].Name;
                    var priority = prioritiesList[i];

                    var instance = Database.IndexStorage.GetIndexInstance(indexName);
                    if (instance == null)
                    {
                        Log.Warn("Couldn't set index priority because index named: '{0}' doesn't exist", indexName);
                        continue;
                    }
                    
                    instance.Priority = priority;
                }

                CreateIndexReplacementDocuments(createdIndexes);

                return createdIndexes;
            }
            catch (Exception e)
            {
                Log.WarnException("Could not create index batch", e);

                if (isReplication == false)
                {
                    foreach (var index in createdIndexes)
                    {
                        DeleteIndex(index.Name);
                        if (index.IsSideBySide)
                            Database.Documents.Delete(index.Name, null, null);
                    }
                }

                throw;
            }
        }

        private void SetIndexesPriority(int[] indexesIds, List<IndexingPriority> prioritiesList)
        {
            for (var retries = 0; retries < 10; retries++)
            {
                try
                {
                    Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexesPriority(indexesIds, prioritiesList.ToArray()));
                    return;
                }
                catch (Exception e)
                {
                    if (TransactionalStorageHelper.IsWriteConflict(e, out e))
                    {
                        Thread.Sleep(100);
                        continue;
                    }

                    throw;
                }
            }

            // we couldn't set the index priority for all indexes at once
            // we'll try to set the index priority one by one

            for (var i = 0; i < indexesIds.Length; i++)
            {
                var indexId = indexesIds[i];
                var priority = prioritiesList[i];
                try
                {
                    Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(indexId, priority));
                }
                catch (Exception e)
                {
                    if (TransactionalStorageHelper.IsWriteConflict(e, out e) == false)
                        throw;

                    var message = $"Could not set index priority to: {priority} for index id: {indexId}";
                    Log.ErrorException(message, e);
                    Database.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = SystemTime.UtcNow,
                        Message = e.Message,
                        Title = message,
                        Exception = e.ToString(),
                        UniqueKey = message
                    });
                }
            }
        }

        private void CreateIndexReplacementDocuments(List<IndexInfo> createdIndexes)
        {
            using (Database.DocumentLock.Lock())
            {
                Database.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var createdIndex in createdIndexes)
                    {
                        if (createdIndex.IsSideBySide == false)
                            continue;

                        Database.Documents.Put(
                            Constants.IndexReplacePrefix + createdIndex.Name,
                            null,
                            RavenJObject.FromObject(new IndexReplaceDocument
                            {
                                IndexToReplace = createdIndex.OriginalName,
                                MinimumEtagBeforeReplace = createdIndex.MinimumEtagBeforeReplace,
                                ReplaceTimeUtc = createdIndex.ReplaceTimeUtc
                            }),
                            new RavenJObject(),
                            null);
                    }
                });
            }
        }

        private IndexLockMode? GetCurrentLockMode(string indexName)
        {
            var currentIndexDefinition = IndexDefinitionStorage.GetIndexDefinition(indexName);
            return currentIndexDefinition?.LockMode;
        }

        public class IndexInfo
        {
            public string Name { get; set; }

            public string OriginalName { get; set; }

            public bool IsSideBySide { get; set; }

            public Etag MinimumEtagBeforeReplace { get; set; }

            public DateTime? ReplaceTimeUtc { get; set; }
        }

        private static void AssertAnalyzersValid(IndexDefinition indexDefinition)
        {
            foreach (var analyzer in indexDefinition.Analyzers)
            {
                //this throws if the type cannot be found
                IndexingExtensions.GetAnalyzerType(analyzer.Key, analyzer.Value);
            }
        }

        internal void PutNewIndexIntoStorage(string name, IndexDefinition definition, bool disableIndex = false)
        {
            Debug.Assert(Database.IndexStorage != null);
            Debug.Assert(TransactionalStorage != null);
            Debug.Assert(WorkContext != null);

            Index index = null;
            TransactionalStorage.Batch(actions =>
            {
                var maxId = 0;
                if (Database.IndexStorage.Indexes.Length > 0)
                {
                    maxId = Database.IndexStorage.Indexes.Max();
                }
                definition.IndexId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions);
                if (definition.IndexId <= maxId)
                {
                    actions.General.SetIdentityValue("IndexId", maxId + 1);
                    definition.IndexId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions);
                }

                IndexDefinitionStorage.RegisterNewIndexInThisSession(name, definition);

                // this has to happen in this fashion so we will expose the in memory status after the commit, but 
                // before the rest of the world is notified about this.

                IndexDefinitionStorage.CreateAndPersistIndex(definition);
                Database.IndexStorage.CreateIndexImplementation(definition);
                index = Database.IndexStorage.GetIndexInstance(definition.IndexId);

                // If we execute multiple indexes at once and want to activate them all at once we will disable the index from the endpoint
                if (disableIndex)
                    index.Priority = IndexingPriority.Disabled;

                //ensure that we don't start indexing it right away, let the precomputation run first, if applicable
                index.IsMapIndexingInProgress = true;
                if (definition.IsTestIndex)
                    index.MarkQueried(); // test indexes should be mark queried, so the cleanup task would not delete them immediately

                InvokeSuggestionIndexing(name, definition, index);

                actions.Indexing.AddIndex(definition.IndexId, definition.IsMapReduce);
            });

            Debug.Assert(index != null);

            Action precomputedTask = null;
            if (WorkContext.RunIndexing &&
                name.Equals(Constants.DocumentsByEntityNameIndex, StringComparison.InvariantCultureIgnoreCase) == false &&
                Database.IndexStorage.HasIndex(Constants.DocumentsByEntityNameIndex) && isPrecomputedBatchForNewIndexIsRunning == false)
            {
                // optimization of handling new index creation when the number of document in a database is significantly greater than
                // number of documents that this index applies to - let us use built-in RavenDocumentsByEntityName to get just appropriate documents

                precomputedTask = TryCreateTaskForApplyingPrecomputedBatchForNewIndex(index, definition);
            }
            else
            {
                index.IsMapIndexingInProgress = false;// we can't apply optimization, so we'll make it eligible for running normally
            }

            // The act of adding it here make it visible to other threads
            // we have to do it in this way so first we prepare all the elements of the 
            // index, then we add it to the storage in a way that make it public
            IndexDefinitionStorage.AddIndex(definition.IndexId, definition);

            // we start the precomputedTask _after_ we finished adding the index
            precomputedTask?.Invoke();

            WorkContext.ShouldNotifyAboutWork(() => "PUT INDEX " + name);
            WorkContext.NotifyAboutWork();       
        }

        private Action TryCreateTaskForApplyingPrecomputedBatchForNewIndex(Index index, IndexDefinition definition)
        {
            if (Database.Configuration.MaxPrecomputedBatchSizeForNewIndex <= 0) //precaution -> should never be lower than 0
            {
                index.IsMapIndexingInProgress = false;
                return null;
            } 
                
            var generator = IndexDefinitionStorage.GetViewGenerator(definition.IndexId);
            if (generator.ForEntityNames.Count == 0 && index.IsTestIndex == false)
            {
                // we don't optimize if we don't have what to optimize _on_, we know this is going to return all docs.
                // no need to try to optimize that, then
                index.IsMapIndexingInProgress = false;
                return null;
            }

            //only one precomputed batch can run at a time except for test indexes
            if (index.IsTestIndex == false)
            {
                lock (precomputedLock)
                {

                    if (isPrecomputedBatchForNewIndexIsRunning)
                    {
                        index.IsMapIndexingInProgress = false;
                        return null;
                    }

                    isPrecomputedBatchForNewIndexIsRunning = true;
                }
            }

            try
            {
                var cts = new CancellationTokenSource();
                var task = new Task(() =>
                {
                    try
                    {
                        ApplyPrecomputedBatchForNewIndex(index, generator, 
                            index.IsTestIndex == false ?
                            Database.Configuration.MaxPrecomputedBatchSizeForNewIndex :  
                            Database.Configuration.Indexing.MaxNumberOfItemsToProcessInTestIndexes, cts);
                    }
                    catch (TotalDataSizeExceededException e)
                    {
                        //expected error
                        Log.Info(e.Message);
                    }
                    catch (Exception e)
                    {
                        Log.Warn("Could not apply precomputed batch for index " + index, e);
                    }
                    finally
                    {
                        if (index.IsTestIndex == false)
                            isPrecomputedBatchForNewIndexIsRunning = false;
                        index.IsMapIndexingInProgress = false;
                        WorkContext.ShouldNotifyAboutWork(() => "Precomputed indexing batch for " + index.PublicName + " is completed");
                        WorkContext.NotifyAboutWork();
                    }
                }, TaskCreationOptions.LongRunning);

                return () =>
                {
                    try
                    {
                        task.Start();

                        long id;
                        Database
                            .Tasks
                            .AddTask(
                                task,
                                new TaskBasedOperationState(task),
                                new TaskActions.PendingTaskDescription
                                {
                                    StartTime = DateTime.UtcNow,
                                    Description = index.PublicName,
                                    TaskType = TaskActions.PendingTaskType.NewIndexPrecomputedBatch
                                },
                                out id,
                                cts);
                    }
                    catch (Exception)
                    {
                        index.IsMapIndexingInProgress = false;
                        if (index.IsTestIndex == false)
                            isPrecomputedBatchForNewIndexIsRunning = false;
                        throw;
                    }
                };
            }
            catch (Exception)
            {
                index.IsMapIndexingInProgress = false;
                if (index.IsTestIndex == false)
                    isPrecomputedBatchForNewIndexIsRunning = false;
                throw;
            }
        }

        private void ApplyPrecomputedBatchForNewIndex(Index index, AbstractViewGenerator generator, int pageSize, CancellationTokenSource cts)
        {
            PrecomputedIndexingBatch result = null;

            var docsToIndex = new List<JsonDocument>();
            TransactionalStorage.Batch(actions =>
            {
                var query = QueryBuilder.GetQueryForAllMatchingDocumentsForIndex(Database, generator.ForEntityNames);

                using (DocumentCacher.SkipSetDocumentsInDocumentCache())
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, WorkContext.CancellationToken))
                using (var op = new QueryActions.DatabaseQueryOperation(Database, Constants.DocumentsByEntityNameIndex, new IndexQuery
                {
                    Query = query,
                    PageSize = pageSize
                }, actions, linked)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();

                    //if we are working on a test index, apply the optimization anyway, as the index is capped by small number of results
                    if (op.Header.TotalResults > pageSize && index.IsTestIndex == false)
                    {
                        // we don't apply this optimization if the total number of results 
                        // to index is more than the max numbers to index in a single batch. 
                        // The idea here is that we need to keep the amount
                        // of memory we use to a manageable level even when introducing a new index to a BIG 
                        // database
                        try
                        {
                            cts.Cancel();
                            // we have to run just a little bit of the query to properly setup the disposal
                            op.Execute(o => { });
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        return;
                    }

                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug("For new index {0}, using precomputed indexing batch optimization for {1} docs",
                            index, op.Header.TotalResults);
                    }

                    var totalLoadedDocumentSize = 0;
                    const int totalSizeToCheck = 16 * 1024 * 1024; //16MB
                    var localLoadedDocumentSize = 0;
                    op.Execute(document =>
                    {
                        var metadata = document.Value<RavenJObject>(Constants.Metadata);
                        var key = metadata.Value<string>("@id");
                        var etag = Etag.Parse(metadata.Value<string>("@etag"));
                        var lastModified = DateTime.Parse(metadata.Value<string>(Constants.LastModified));
                        document.Remove(Constants.Metadata);
                        var serializedSizeOnDisk = metadata.Value<int>(Constants.SerializedSizeOnDisk);
                        metadata.Remove(Constants.SerializedSizeOnDisk);

                        var doc = new JsonDocument
                        {
                            DataAsJson = document,
                            Etag = etag,
                            Key = key,
                            SerializedSizeOnDisk = serializedSizeOnDisk,
                            LastModified = lastModified,
                            SkipDeleteFromIndex = true,
                            Metadata = metadata
                        };

                        docsToIndex.Add(doc);
                        totalLoadedDocumentSize += serializedSizeOnDisk;
                        localLoadedDocumentSize += serializedSizeOnDisk;

                        if (totalLoadedDocumentSize > Database.Configuration.MaxPrecomputedBatchTotalDocumentSizeInBytes)
                        {
                            var error = $"Aborting applying precomputed batch for index id: {index.indexId}, " +
                                        $"name: {index.PublicName} because we have {totalLoadedDocumentSize}MB of documents that were fetched" + 
                                        $"and the configured max data to fetch is " +
                                        $"{Database.Configuration.MaxPrecomputedBatchTotalDocumentSizeInBytes/1024/1024}MB";

                            //we are aborting operation, so don't keep the references
                            docsToIndex.Clear();
                            throw new TotalDataSizeExceededException(error);
                        }


                        if (localLoadedDocumentSize <= totalSizeToCheck)
                            return;

                        localLoadedDocumentSize = 0;

                        if (Database.Configuration.MemoryLimitForProcessingInMb > MemoryStatistics.AvailableMemoryInMb)
                        {
                            var error = $"Aborting applying precomputed batch for index id: {index.indexId}, " +
                                        $"name: {index.PublicName} because we have {MemoryStatistics.AvailableMemoryInMb}MB " +
                                        $"of available memory and the available memory for processing is: " +
                                        $"{Database.Configuration.MemoryLimitForProcessingInMb}MB";

                            //we are aborting operation, so don't keep the references
                            docsToIndex.Clear();
                            throw new TotalDataSizeExceededException(error);
                        }
                    });

                    result = new PrecomputedIndexingBatch
                    {
                        LastIndexed = op.Header.IndexEtag,
                        LastModified = op.Header.IndexTimestamp,
                        Documents = docsToIndex,
                        Index = index
                    };
                }
            });

            if (result != null && result.Documents != null && result.Documents.Count >= 0)
            {
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, WorkContext.CancellationToken))
                {
                    Database.IndexingExecuter.IndexPrecomputedBatch(result, linked.Token);

                    if (index.IsTestIndex)
                        TransactionalStorage.Batch(accessor => accessor.Indexing.TouchIndexEtag(index.IndexId));
                }
            }

        }

        private void InvokeSuggestionIndexing(string name, IndexDefinition definition, Index index)
        {
            foreach (var suggestion in definition.SuggestionsOptions)
            {
                var field = suggestion;

                var indexExtensionKey = MonoHttpUtility.UrlEncode(field);

                var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
                    index,
                     WorkContext,
                     Path.Combine(Database.Configuration.IndexStoragePath, "Raven-Suggestions", name, indexExtensionKey),
                     Database.Configuration.RunInMemory,
                     field);

                Database.IndexStorage.SetIndexExtension(name, indexExtensionKey, suggestionQueryIndexExtension);
            }
        }

        private IndexCreationOptions FindIndexCreationOptions(IndexDefinition definition, ref string name)
        {
            definition.Name = name;
            definition.RemoveDefaultValues();
            IndexDefinitionStorage.ResolveAnalyzers(definition);
            var findIndexCreationOptions = IndexDefinitionStorage.FindIndexCreationOptions(definition);
            return findIndexCreationOptions;
        }

        public RavenJArray GetIndexNames(int start, int pageSize)
        {
            return new RavenJArray(
                IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                    .Select(s => new RavenJValue(s))
                );
        }

        public RavenJArray GetIndexNamesWithType(int start, int pageSize)
        {
            return new RavenJArray(
                from indexName in IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                let indexDefinition = IndexDefinitionStorage.GetIndexDefinition(indexName)
                select new RavenJObject
                {
                    {"Name", new RavenJValue(indexName)},
                    {"IsMapReduce", indexDefinition?.IsMapReduce ?? false},
                });
        }

        public RavenJArray GetIndexes(int start, int pageSize)
        {
            return new RavenJArray(
                from indexName in IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                let indexDefinition = IndexDefinitionStorage.GetIndexDefinition(indexName)
                select new RavenJObject
                {
                    {"name", new RavenJValue(indexName)},
                    {"definition", indexDefinition != null ? RavenJObject.FromObject(indexDefinition) : null},
                });
        }

        public IndexDefinition GetIndexDefinition(string index)
        {
            return IndexDefinitionStorage.GetIndexDefinition(index);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ResetIndex(string index)
        {
            var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
            if (indexDefinition == null)
                throw new InvalidOperationException("There is no index named: " + index);

            DeleteIndex(index);

            //treat it like an update to the current index
            indexDefinition.IndexVersion = (indexDefinition.IndexVersion ?? 0) + 1;

            PutIndex(index, indexDefinition);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool DeleteIndex(string name, int? deletedIndexVersion = null)
        {
            var instance = IndexDefinitionStorage.GetIndexDefinition(name);
            if (instance == null)
                return false;

            DeleteIndex(instance, deletedIndexVersion: deletedIndexVersion);
            return true;
        }

        internal void DeleteIndex(IndexDefinition instance, bool removeByNameMapping = true, 
            bool clearErrors = true, bool removeIndexReplaceDocument = true, 
            bool isSideBySideReplacement = false, int? deletedIndexVersion = null)
        {
            using (IndexDefinitionStorage.TryRemoveIndexContext())
            {
                if (instance == null)
                    return;

                var currentIndexVersion = instance.IndexVersion;
                if (deletedIndexVersion != null &&
                    currentIndexVersion != null &&
                    currentIndexVersion > deletedIndexVersion)
                {
                    // the index version is larger than the deleted one
                    // got an old delete from an outdated node
                    return;
                }

                // Set up a flag to signal that this is something we're doing
                TransactionalStorage.Batch(actions =>
                {
                    actions.Lists.Set("Raven/Indexes/PendingDeletion", 
                        instance.IndexId.ToString(CultureInfo.InvariantCulture),
                        RavenJObject.FromObject(new
                        {
                            TimeOfOriginalDeletion = SystemTime.UtcNow,
                            instance.IndexId,
                            IndexName = instance.Name,
                            instance.IndexVersion
                        }), UuidType.Tasks);
                });

                // Delete the main record synchronously
                IndexDefinitionStorage.RemoveIndex(instance.IndexId, removeByNameMapping);
                Database.IndexStorage.DeleteIndex(instance.IndexId);

                if (clearErrors)
                    WorkContext.ClearErrorsFor(instance.Name);

                var isSideBySide = instance.IsSideBySideIndex ||
                                   instance.Name.StartsWith(Constants.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase);
                if (removeIndexReplaceDocument && isSideBySide)
                {
                    Database.Documents.Delete(Constants.IndexReplacePrefix + instance.Name, null, null);
                }

                Database.IndexStorage.DeleteSuggestionsData(instance.Name);

                //remove the header information in a sync process
                PrepareIndexDeletion(instance.IndexId);

                //and delete the data in the background
                Database.Maintenance.StartDeletingIndexDataAsync(instance.IndexId, instance.Name);

                var indexChangeType = isSideBySideReplacement ? IndexChangeTypes.SideBySideReplace : IndexChangeTypes.IndexRemoved;

                var version = currentIndexVersion ?? 0;
                if (deletedIndexVersion != null)
                    version = Math.Max(version, deletedIndexVersion.Value);

                TransactionalStorage.Batch(actions =>
                {
                    if (instance.Name.StartsWith(Constants.SideBySideIndexNamePrefix))
                        return;

                    var metadata = new RavenJObject
                    {
                        {IndexDefinitionStorage.IndexVersionKey, version }
                    };
                    actions.Lists.Set(Constants.RavenDeletedIndexesVersions, instance.Name, metadata, UuidType.Indexing);
                });

                // We raise the notification now because as far as we're concerned it is done *now*
                TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
                    Database.Notifications.RaiseNotifications(new IndexChangeNotification
                    {
                        Name = instance.Name,
                        Type = indexChangeType,
                        Version = instance.IndexVersion
                    })
                );
            }
        }

        private void PrepareIndexDeletion(int indexId)
        {
            var retries = 0;
            while (true)
            {
                try
                {
                    TransactionalStorage.Batch(actions => actions.Indexing.PrepareIndexForDeletion(indexId));
                    return;
                }
                catch (Exception e)
                {
                    if (TransactionalStorageHelper.IsWriteConflict(e, out e))
                    {
                        if (++retries >= 10)
                            throw;

                        Thread.Sleep(100);
                        continue;
                    }

                    throw;
                }
            }
        }

        public void RenameIndex(IndexDefinition instance, string newIndexName)
        {
            switch (instance.LockMode)
            {
                case IndexLockMode.LockedIgnore:
                    return;
                case IndexLockMode.LockedError:
                    throw new InvalidOperationException("An attempt to rename an index, locked with LockedError was detected.");
            }

            using (Database.IndexDefinitionStorage.TryRemoveIndexContext())
            {
                var existingIndexName = instance.Name;
                if (Database.IndexDefinitionStorage.RenameIndex(existingIndexName, newIndexName))
                {
                    Database.IndexStorage.RenameIndex(instance, newIndexName);

                    using (Database.DocumentLock.Lock())
                    {
                        TransactionalStorage.Batch(actions =>
                        {
                            var scriptedIndexSetup = actions.Documents.DocumentByKey(ScriptedIndexResults.IdPrefix + existingIndexName);

                            if (scriptedIndexSetup != null)
                            {
                                actions.Documents.AddDocument(ScriptedIndexResults.IdPrefix + newIndexName, null, scriptedIndexSetup.DataAsJson, scriptedIndexSetup.Metadata);

                                Etag etag;
                                RavenJObject metadata;
                                actions.Documents.DeleteDocument(ScriptedIndexResults.IdPrefix + existingIndexName, scriptedIndexSetup.Etag, out metadata, out etag);
                            }

                            WorkContext.HandleIndexRename(existingIndexName, newIndexName, instance.IndexId, actions);
                        });
                    }
                }
            }
        }
    }
}
