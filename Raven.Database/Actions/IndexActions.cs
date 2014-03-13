// -----------------------------------------------------------------------
//  <copyright file="IndexActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class IndexActions
    {
        public Etag GetIndexEtag(string indexName, Etag previousEtag, string resultTransformer = null)
        {
            Etag lastDocEtag = Etag.Empty;
            Etag lastReducedEtag = null;
            bool isStale = false;
            int touchCount = 0;
            TransactionalStorage.Batch(accessor =>
            {
                var indexInstance = IndexStorage.GetIndexInstance(indexName);
                if (indexInstance == null)
                    return;
                isStale = (indexInstance.IsMapIndexingInProgress) ||
                          accessor.Staleness.IsIndexStale(indexInstance.indexId, null, null);
                lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                var indexStats = accessor.Indexing.GetIndexStats(indexInstance.indexId);
                if (indexStats != null)
                {
                    lastReducedEtag = indexStats.LastReducedEtag;
                }
                touchCount = accessor.Staleness.GetIndexTouchCount(indexInstance.indexId);
            });


            var indexDefinition = GetIndexDefinition(indexName);
            if (indexDefinition == null)
                return Etag.Empty; // this ensures that we will get the normal reaction of IndexNotFound later on.
            using (var md5 = MD5.Create())
            {
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

                var indexEtag = Etag.Parse(md5.ComputeHash(list.ToArray()));

                if (previousEtag != null && previousEtag != indexEtag)
                {
                    // the index changed between the time when we got it and the time 
                    // we actually call this, we need to return something random so that
                    // the next time we won't get 304

                    return Etag.InvalidEtag;
                }

                return indexEtag;
            }
        }

       

        internal void CheckReferenceBecauseOfDocumentUpdate(string key, IStorageActionsAccessor actions)
        {
            TouchedDocumentInfo touch;
            recentTouches.TryRemove(key, out touch);

            foreach (var referencing in actions.Indexing.GetDocumentsReferencing(key))
            {
                Etag preTouchEtag = null;
                Etag afterTouchEtag = null;
                try
                {
                    actions.Documents.TouchDocument(referencing, out preTouchEtag, out afterTouchEtag);
                }
                catch (ConcurrencyException)
                {
                }

                if (preTouchEtag == null || afterTouchEtag == null)
                    continue;

                actions.General.MaybePulseTransaction();

                recentTouches.Set(referencing, new TouchedDocumentInfo
                {
                    PreTouchEtag = preTouchEtag,
                    TouchedEtag = afterTouchEtag
                });
            }
        }

        // only one index can be created at any given time
        // the method already handle attempts to create the same index, so we don't have to 
        // worry about this.
        [MethodImpl(MethodImplOptions.Synchronized)]
        public string PutIndex(string name, IndexDefinition definition)
        {
            if (name == null)
                throw new ArgumentNullException("name");

            var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);

            if (existingIndex != null)
            {
                switch (existingIndex.LockMode)
                {
                    case IndexLockMode.LockedIgnore:
                        log.Info("Index {0} not saved because it was lock (with ignore)", name);
                        return name;

                    case IndexLockMode.LockedError:
                        throw new InvalidOperationException("Can not overwrite locked index: " + name);
                }
            }

            name = name.Trim();

            switch (FindIndexCreationOptions(definition, ref name))
            {
                case IndexCreationOptions.Noop:
                    return name;
                case IndexCreationOptions.Update:
                    // ensure that the code can compile
                    new DynamicViewCompiler(definition.Name, definition, Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Configuration).GenerateInstance();
                    DeleteIndex(name);
                    break;
            }

            PutNewIndexIntoStorage(name, definition);

            workContext.ClearErrorsFor(name);

            TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => RaiseNotifications(new IndexChangeNotification
            {
                Name = name,
                Type = IndexChangeTypes.IndexAdded,
            }));

            return name;
        }



        internal void PutNewIndexIntoStorage(string name, IndexDefinition definition)
        {
            Debug.Assert(IndexStorage != null);
            Debug.Assert(TransactionalStorage != null);
            Debug.Assert(workContext != null);

            TransactionalStorage.Batch(actions =>
            {
                definition.IndexId = (int)GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions, null);
                IndexDefinitionStorage.RegisterNewIndexInThisSession(name, definition);

                // this has to happen in this fashion so we will expose the in memory status after the commit, but 
                // before the rest of the world is notified about this.

                IndexDefinitionStorage.CreateAndPersistIndex(definition);
                IndexStorage.CreateIndexImplementation(definition);

                InvokeSuggestionIndexing(name, definition);

                actions.Indexing.AddIndex(definition.IndexId, definition.IsMapReduce);
            });

            if (name.Equals(Constants.DocumentsByEntityNameIndex, StringComparison.InvariantCultureIgnoreCase) == false &&
                IndexStorage.HasIndex(Constants.DocumentsByEntityNameIndex))
            {
                // optimization of handling new index creation when the number of document in a database is significantly greater than
                // number of documents that this index applies to - let us use built-in RavenDocumentsByEntityName to get just appropriate documents

                var index = IndexStorage.GetIndexInstance(definition.IndexId);
                TryApplyPrecomputedBatchForNewIndex(index, definition);
            }

            workContext.ShouldNotifyAboutWork(() => "PUT INDEX " + name);
            WorkContext.NotifyAboutWork();
            // The act of adding it here make it visible to other threads
            // we have to do it in this way so first we prepare all the elements of the 
            // index, then we add it to the storage in a way that make it public
            IndexDefinitionStorage.AddIndex(definition.IndexId, definition);
        }


        private void TryApplyPrecomputedBatchForNewIndex(Index index, IndexDefinition definition)
        {
            var generator = IndexDefinitionStorage.GetViewGenerator(definition.IndexId);
            if (generator.ForEntityNames.Count == 0)
            {
                // we don't optimize if we don't have what to optimize _on, we know this is going to return all docs.
                // no need to try to optimize that, then
                return;
            }

            index.IsMapIndexingInProgress = true;
            try
            {
                Task.Factory.StartNew(() => ApplyPrecomputedBatchForNewIndex(index, generator),
                    TaskCreationOptions.LongRunning)
                    .ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            log.Warn("Could not apply precomputed batch for index " + index, t.Exception);
                        }
                        index.IsMapIndexingInProgress = false;
                        WorkContext.ShouldNotifyAboutWork(() => "Precomputed indexing batch for " + index.PublicName + " is completed");
                        WorkContext.NotifyAboutWork();

                    });
            }
            catch (Exception)
            {
                index.IsMapIndexingInProgress = false;
                throw;
            }
        }


        private void ApplyPrecomputedBatchForNewIndex(Index index, AbstractViewGenerator generator)
        {
            const string documentsByEntityNameIndex = "Raven/DocumentsByEntityName";

            PrecomputedIndexingBatch result = null;

            var docsToIndex = new List<JsonDocument>();
            TransactionalStorage.Batch(actions =>
            {
                var countOfDocuments = actions.Documents.GetDocumentsCount();

                var tags = generator.ForEntityNames.Select(entityName => "Tag:[[" + entityName + "]]").ToList();

                var query = string.Join(" OR ", tags);
                var stats =
                    actions.Indexing.GetIndexStats(
                        IndexDefinitionStorage.GetIndexDefinition(documentsByEntityNameIndex).IndexId);

                var lastIndexedEtagByRavenDocumentsByEntityName = stats.LastIndexedEtag;
                var lastModifiedByRavenDocumentsByEntityName = stats.LastIndexedTimestamp;

                var cts = new CancellationTokenSource();
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, WorkContext.CancellationToken))
                using (var op = new DatabaseQueryOperation(this, documentsByEntityNameIndex, new IndexQuery
                {
                    Query = query
                }, actions, linked.Token)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();
                    if (op.Header.TotalResults == 0 ||
                        op.Header.TotalResults > (countOfDocuments * 0.25) ||
                        (op.Header.TotalResults > Configuration.MaxNumberOfItemsToIndexInSingleBatch * 4))
                    {
                        // we don't apply this optimization if the total number of results is more than
                        // 25% of the count of documents (would be easier to just run it regardless).
                        // or if the number of docs to index is significantly more than the max numbers
                        // to index in a single batch. The idea here is that we need to keep the amount
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

                    log.Debug("For new index {0}, using precomputed indexing batch optimization for {1} docs", index,
                              op.Header.TotalResults);
                    op.Execute(document =>
                    {
                        var metadata = document.Value<RavenJObject>(Constants.Metadata);
                        var key = metadata.Value<string>("@id");
                        var etag = Etag.Parse(metadata.Value<string>("@etag"));
                        var lastModified = DateTime.Parse(metadata.Value<string>(Constants.LastModified));
                        document.Remove(Constants.Metadata);

                        docsToIndex.Add(new JsonDocument()
                        {
                            DataAsJson = document,
                            Etag = etag,
                            Key = key,
                            LastModified = lastModified,
                            SkipDeleteFromIndex = true,
                            Metadata = metadata
                        });
                    });
                }

                result = new PrecomputedIndexingBatch()
                {
                    LastIndexed = lastIndexedEtagByRavenDocumentsByEntityName,
                    LastModified = lastModifiedByRavenDocumentsByEntityName,
                    Documents = docsToIndex,
                    Index = index
                };
            });

            if (result != null && result.Documents != null && result.Documents.Count > 0)
                indexingExecuter.IndexPrecomputedBatch(result);

        }

        private void InvokeSuggestionIndexing(string name, IndexDefinition definition)
        {
            foreach (var suggestion in definition.Suggestions)
            {
                var field = suggestion.Key;
                var suggestionOption = suggestion.Value;

                if (suggestionOption.Distance == StringDistanceTypes.None)
                    continue;

                var indexExtensionKey =
                    MonoHttpUtility.UrlEncode(field + "-" + suggestionOption.Distance + "-" +
                                              suggestionOption.Accuracy);

                var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
                     workContext,
                     Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", name, indexExtensionKey),
                     SuggestionQueryRunner.GetStringDistance(suggestionOption.Distance),
                     configuration.RunInMemory,
                     field,
                     suggestionOption.Accuracy);

                IndexStorage.SetIndexExtension(name, indexExtensionKey, suggestionQueryIndexExtension);
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

        private void RemoveFromCurrentlyRunningQueryList(string index, ExecutingQueryInfo queryStat)
        {
            ConcurrentSet<ExecutingQueryInfo> set;
            if (workContext.CurrentlyRunningQueries.TryGetValue(index, out set) == false)
                return;
            set.TryRemove(queryStat);
        }

        private ExecutingQueryInfo AddToCurrentlyRunningQueryList(string index, IndexQuery query)
        {
            var set = workContext.CurrentlyRunningQueries.GetOrAdd(index, x => new ConcurrentSet<ExecutingQueryInfo>());
            var queryStartTime = DateTime.UtcNow;
            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query);
            set.Add(executingQueryInfo);
            return executingQueryInfo;
        }

        private IEnumerable<RavenJObject> GetQueryResults(IndexQuery query,
            AbstractViewGenerator viewGenerator,
            DocumentRetriever docRetriever,
            IEnumerable<JsonDocument> results,
            List<string> transformerErrors,
            CancellationToken token)
        {
            if (query.PageSize <= 0) // maybe they just want the stats? 
            {
                return Enumerable.Empty<RavenJObject>();
            }

            IndexingFunc transformFunc = null;

            // Check an explicitly declared one first
            if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
            {
                var transformGenerator = IndexDefinitionStorage.GetTransformer(query.ResultsTransformer);

                if (transformGenerator != null && transformGenerator.TransformResultsDefinition != null)
                    transformFunc = transformGenerator.TransformResultsDefinition;
                else
                    throw new InvalidOperationException("The transformer " + query.ResultsTransformer + " was not found");
            }
            else if (query.SkipTransformResults == false && viewGenerator.TransformResultsDefinition != null)
            {
                transformFunc = source => viewGenerator.TransformResultsDefinition(docRetriever, source);
            }

            if (transformFunc == null)
                return results.Select(x => x.ToJson());

            var dynamicJsonObjects = results.Select(x => new DynamicLuceneOrParentDocumntObject(docRetriever, x.ToJson()));
            var robustEnumerator = new RobustEnumerator(token, 100)
            {
                OnError =
                    (exception, o) =>
                    transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o),
                                                        exception.Message))
            };
            return robustEnumerator.RobustEnumeration(
                dynamicJsonObjects.Cast<object>().GetEnumerator(),
                transformFunc)
                .Select(JsonExtensions.ToJObject);
        }

        internal Task StartDeletingIndexDataAsync(int id)
        {
            //remove the header information in a sync process
            TransactionalStorage.Batch(actions => actions.Indexing.PrepareIndexForDeletion(id));
            var deleteIndexTask = Task.Run(() =>
            {
                Debug.Assert(IndexStorage != null);
                IndexStorage.DeleteIndexData(id); // Data can take a while

                TransactionalStorage.Batch(actions =>
                {
                    // And Esent data can take a while too
                    actions.Indexing.DeleteIndex(id, WorkContext.CancellationToken);
                    if (WorkContext.CancellationToken.IsCancellationRequested)
                        return;

                    actions.Lists.Remove("Raven/Indexes/PendingDeletion", id.ToString(CultureInfo.InvariantCulture));
                });
            });

            long taskId;
            AddTask(deleteIndexTask, null, out taskId);

            PendingTaskAndState value;
            deleteIndexTask.ContinueWith(_ => pendingTasks.TryRemove(taskId, out value));

            return deleteIndexTask;
        }

       

        

        public RavenJArray GetIndexNames(int start, int pageSize)
        {
            return new RavenJArray(
                IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                    .Select(s => new RavenJValue(s))
                );
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

        public void ResetIndex(string index)
        {
            var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
            if (indexDefinition == null)
                throw new InvalidOperationException("There is no index named: " + index);
            DeleteIndex(index);
            PutIndex(index, indexDefinition);
        }

        public void DeleteIndex(string name)
        {
            using (IndexDefinitionStorage.TryRemoveIndexContext())
            {
                var instance = IndexDefinitionStorage.GetIndexDefinition(name);
                if (instance == null) return;

                // Set up a flag to signal that this is something we're doing
                TransactionalStorage.Batch(actions => actions.Lists.Set("Raven/Indexes/PendingDeletion", instance.IndexId.ToString(CultureInfo.InvariantCulture), (RavenJObject.FromObject(new
                {
                    TimeOfOriginalDeletion = SystemTime.UtcNow,
                    instance.IndexId
                })), UuidType.Tasks));

                // Delete the main record synchronously
                IndexDefinitionStorage.RemoveIndex(name);
                IndexStorage.DeleteIndex(instance.IndexId);

                ConcurrentSet<string> _;
                workContext.DoNotTouchAgainIfMissingReferences.TryRemove(instance.IndexId, out _);
                workContext.ClearErrorsFor(name);

                // And delete the data in the background
                StartDeletingIndexDataAsync(instance.IndexId);

                // We raise the notification now because as far as we're concerned it is done *now*
                TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => RaiseNotifications(new IndexChangeNotification
                {
                    Name = name,
                    Type = IndexChangeTypes.IndexRemoved,
                }));
            }
        }

    }
}