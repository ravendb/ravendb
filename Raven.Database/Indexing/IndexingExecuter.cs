//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    public class IndexingExecuter : AbstractIndexingExecuter
    {
        public IndexingExecuter(ITransactionalStorage transactionalStorage, WorkContext context, TaskScheduler scheduler)
            : base(transactionalStorage, context, scheduler)
        {
        }

        protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions)
        {
            return actions.Staleness.IsMapStale(indexesStat.Name);
        }

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexName = indexesStat.Name,
				LastIndexedEtag = indexesStat.LastIndexedEtag
			};
		}


        protected override void ExecuteIndexingWorkOnMultipleThreads(IEnumerable<IndexToWorkOn> indexesToWorkOn)
        {
            ExecuteIndexingInternal(indexesToWorkOn, documents => Parallel.ForEach(indexesToWorkOn, new ParallelOptions
            {
                MaxDegreeOfParallelism = context.Configuration.MaxNumberOfParallelIndexTasks,
                TaskScheduler = scheduler
            }, indexToWorkOn => transactionalStorage.Batch(actions => IndexDocuments(actions, indexToWorkOn.IndexName, documents))));
        }

        protected override void ExecuteIndexingWorkOnSingleThread(IEnumerable<IndexToWorkOn> indexesToWorkOn)
        {
            ExecuteIndexingInternal(indexesToWorkOn, jsonDocs =>
            {
                foreach (var indexToWorkOn in indexesToWorkOn)
                {
                    var copy = indexToWorkOn;
                    transactionalStorage.Batch(
                        actions => IndexDocuments(actions, copy.IndexName, jsonDocs));
                }
            });
        }

        private void ExecuteIndexingInternal(IEnumerable<IndexToWorkOn> indexesToWorkOn, Action<JsonDocument[]> indexingOp)
        {
            var lastIndexedGuidForAllIndexes = indexesToWorkOn.Min(x => new ComparableByteArray(x.LastIndexedEtag.ToByteArray())).ToGuid();

            JsonDocument[] jsonDocs = null;
            try
            {
                transactionalStorage.Batch(actions =>
                {
                    jsonDocs = actions.Documents.GetDocumentsAfter(lastIndexedGuidForAllIndexes)
                        .Where(x => x != null)
                        .Select(doc =>
                        {
                            DocumentRetriever.EnsureIdInMetadata(doc);
                            return doc;
                        })
                        .Take(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch) // ensure that we won't go overboard with reading and blow up with OOM
                        .ToArray();
                });

                if (jsonDocs.Length > 0)
                    indexingOp(jsonDocs);
            }
            finally
            {
                if (jsonDocs != null && jsonDocs.Length > 0)
                {
                    var last = jsonDocs.Last();
                    var lastEtag = last.Etag;
                    var lastModified = last.LastModified;

                    var lastIndexedEtag = new ComparableByteArray(lastEtag.ToByteArray());
                    // whatever we succeeded in indexing or not, we have to update this
                    // because otherwise we keep trying to re-index failed documents
                    transactionalStorage.Batch(actions =>
                    {
                        foreach (var indexToWorkOn in indexesToWorkOn)
                        {
                            if (new ComparableByteArray(indexToWorkOn.LastIndexedEtag.ToByteArray()).CompareTo(lastIndexedEtag) > 0)
                                continue;
                            actions.Indexing.UpdateLastIndexed(indexToWorkOn.IndexName, lastEtag, lastModified);
                        }
                    });
                }
            }
        }

        protected override bool IsValidIndex(IndexStats indexesStat)
        {
            return true;
        }


        private void IndexDocuments(IStorageActionsAccessor actions, string index, JsonDocument[] jsonDocs)
        {
            var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
            if (viewGenerator == null)
                return; // index was deleted, probably

            var dateTime = jsonDocs.Min(x => x.LastModified);

            var documentRetriever = new DocumentRetriever(null, context.ReadTriggers);
            try
            {
                log.DebugFormat("Indexing {0} documents for index: {1}", jsonDocs.Length, index);
                context.IndexStorage.Index(index, viewGenerator,
                    jsonDocs
                    .Select(doc => documentRetriever
                        .ExecuteReadTriggers(doc, null, ReadOperation.Index))
                    .Where(doc => doc != null)
                    .Select(x => JsonToExpando.Convert(x.ToJson())), context, actions, dateTime);
            }
            catch (Exception e)
            {
                if (actions.IsWriteConflict(e))
                    return;
                log.WarnFormat(e, "Failed to index documents for index: {0}", index);
            }
        }

    }
}