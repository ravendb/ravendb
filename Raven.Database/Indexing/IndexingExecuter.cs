//-----------------------------------------------------------------------
// <copyright file="TaskExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class IndexingExecuter
	{
		private readonly WorkContext context;
		private readonly TaskScheduler scheduler;
		private readonly ILog log = LogManager.GetLogger(typeof(IndexingExecuter));
		private readonly ITransactionalStorage transactionalStorage;

		public IndexingExecuter(ITransactionalStorage transactionalStorage, WorkContext context, TaskScheduler scheduler)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
			this.scheduler = scheduler;
		}

		int workCounter;
		private int lastFlushedWorkCounter;

		public void Execute()
		{
			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = ExecuteIndexing();
				}
				catch (Exception e)
				{
					log.Error("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeSpan.FromHours(1), ref workCounter, FlushIndexes);
				}
				else // notify the tasks executer that it has work to do
				{
					context.NotifyAboutWork();
				}
			}
		}

		private void FlushIndexes()
		{
			if (lastFlushedWorkCounter == workCounter || context.DoWork == false)
				return;
			lastFlushedWorkCounter = workCounter;
			context.IndexStorage.FlushAllIndexes();
		}

		private bool ExecuteIndexing()
		{
			var indexesToWorkOn = new List<IndexToWorkOn>();
			transactionalStorage.Batch(actions =>
			{
				foreach (var indexesStat in actions.Indexing.GetIndexesStats())
				{
					var failureRate = actions.Indexing.GetFailureRate(indexesStat.Name);
					if (failureRate.IsInvalidIndex)
					{
						log.InfoFormat("Skipped indexing documents for index: {0} because failure rate is too high: {1}",
									   indexesStat.Name,
									   failureRate.FailureRate);
						continue;
					}
					if (!actions.Staleness.IsIndexStale(indexesStat.Name, null, null))
						continue;
					indexesToWorkOn.Add(new IndexToWorkOn
					{
						IndexName = indexesStat.Name,
						LastIndexedEtag = indexesStat.LastIndexedEtag
					});
				}
			});

			if (indexesToWorkOn.Count == 0)
				return false;

			if(context.Configuration.MaxNumberOfParallelIndexTasks == 1)
				ExecuteIndexingWorkOnSingleThread(indexesToWorkOn);
			else
				ExecuteIndexingWorkOnMultipleThreads(indexesToWorkOn);

			return true;
		}

		private void ExecuteIndexingWorkOnMultipleThreads(IEnumerable<IndexToWorkOn> indexesToWorkOn)
		{
			ExecuteIndexingInternal(indexesToWorkOn, documents => Parallel.ForEach(indexesToWorkOn, new ParallelOptions
			{
				MaxDegreeOfParallelism = context.Configuration.MaxNumberOfParallelIndexTasks,
				TaskScheduler = scheduler
			}, indexToWorkOn => transactionalStorage.Batch(actions => IndexDocuments(actions, indexToWorkOn.IndexName, documents))));
		}

		private void ExecuteIndexingWorkOnSingleThread(IEnumerable<IndexToWorkOn> indexesToWorkOn)
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
						.Select(doc=>
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

		public class IndexToWorkOn
		{
			public string IndexName { get; set; }
			public Guid LastIndexedEtag { get; set; }

			public override string ToString()
			{
				return string.Format("IndexName: {0}, LastIndexedEtag: {1}", IndexName, LastIndexedEtag);
			}
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
						.ProcessReadVetoes(doc, null, ReadOperation.Index))
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

		private class ComparableByteArray : IComparable<ComparableByteArray>, IComparable
		{
			private readonly byte[] inner;

			public ComparableByteArray(byte[] inner)
			{
				this.inner = inner;
			}

			public int CompareTo(ComparableByteArray other)
			{
				if (inner.Length != other.inner.Length)
					return inner.Length - other.inner.Length;
				for (int i = 0; i < inner.Length; i++)
				{
					if (inner[i] != other.inner[i])
						return inner[i] - other.inner[i];
				}
				return 0;
			}

			public int CompareTo(object obj)
			{
				return CompareTo((ComparableByteArray)obj);
			}

			public Guid ToGuid()
			{
				return new Guid(inner);
			}
		}
	}
}
