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
using Task = Raven.Database.Tasks.Task;
using ThreadingTask = System.Threading.Tasks.Task;

namespace Raven.Database.Indexing
{
	public class TaskExecuter
	{
		private readonly WorkContext context;
		private readonly ILog log = LogManager.GetLogger(typeof (TaskExecuter));
		private readonly ITransactionalStorage transactionalStorage;

		public TaskExecuter(ITransactionalStorage transactionalStorage, WorkContext context)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
		}

        int workCounter;
        
        public void Execute()
		{
		    while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork |= ExecuteTasks();
					if (foundWork == false)
						foundWork |= ExecuteIndexing();
				}
				catch (Exception e)
				{
					log.Error("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
				    context.WaitForWork(TimeSpan.FromHours(1), ref workCounter);
				}
			}
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
			
			ExecuteIndexingWorkOnMultipleThreads(indexesToWorkOn);

			return true;
		}

		private void ExecuteIndexingWorkOnMultipleThreads(IEnumerable<IndexToWorkOn> indexesToWorkOn)
		{
            Parallel.ForEach(indexesToWorkOn, indexToWorkOn => 
                transactionalStorage.Batch(actions => 
                    IndexDocuments(actions, indexToWorkOn.IndexName, indexToWorkOn.LastIndexedEtag)));
		}

		private bool ExecuteTasks()
		{
			bool foundWork = false;
			int tasks = 0;
			transactionalStorage.Batch(actions =>
			{
				Task task = actions.Tasks.GetMergedTask(out tasks);
				if (task == null)
					return;

				log.DebugFormat("Executing {0}", task);
				foundWork = true;

				try
				{
					task.Execute(context);
				}
				catch (Exception e)
				{
					log.WarnFormat(e, "Task {0} has failed and was deleted without completing any work", task);
				}
			});
			return foundWork;
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

		public bool IndexDocuments(IStorageActionsAccessor actions, string index, Guid etagToIndexFrom)
		{
			log.DebugFormat("Indexing documents for {0}, etag to index from: {1}", index, etagToIndexFrom);
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
				return false; // index was deleted, probably

			var jsonDocs = actions.Documents.GetDocumentsAfter(etagToIndexFrom)
				.Where(x => x != null)
				.Take(10000) // ensure that we won't go overboard with reading and blow up with OOM
				.ToArray();

			if(jsonDocs.Length == 0)
				return false;

			var dateTime = jsonDocs.Select(x=>x.LastModified).Min();

			var documentRetriever = new DocumentRetriever(null, context.ReadTriggers);
			try
			{
				log.DebugFormat("Indexing {0} documents for index: {1}", jsonDocs.Length, index);
				context.IndexStorage.Index(index, viewGenerator, 
					jsonDocs
					.Select(doc => documentRetriever.ProcessReadVetoes(doc, null, ReadOperation.Index))
					.Where(doc => doc != null)
					.Select(x => JsonToExpando.Convert(x.ToJson())), context, actions, dateTime);

				return true;
			}
			catch (Exception e)
			{
				log.WarnFormat(e, "Failed to index documents for index: {0}", index);
				return false;
			}
			finally
			{
				// whatever we succeeded in indexing or not, we have to update this
				// because otherwise we keep trying to re-index failed documents
				var last = jsonDocs.Last();
				actions.Indexing.UpdateLastIndexed(index, last.Etag, last.LastModified);
			}
			
		}
	}
}
