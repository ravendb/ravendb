using System;
using System.Diagnostics;
using System.Linq;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Tasks;

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

		public void Execute()
		{
			while (context.DoWork)
			{
				var foundWork = false;
				Task task = null;
				try
				{
					int tasks = 0;
					transactionalStorage.Batch(actions =>
					{
						task = actions.Tasks.GetMergedTask(out tasks);
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
					context.PerformanceCounters.IncrementProcessedTask(tasks);
					transactionalStorage.Batch(actions =>
					{
						foreach (var indexesStat in actions.Indexing.GetIndexesStats())
						{
							if (!actions.Tasks.IsIndexStale(indexesStat.Name, null)) 
								continue;
							// in order to ensure fairness, we only process one stale index 
							// then move to process pending tasks, then process more staleness
							if (IndexDocuments(actions, indexesStat.Name, indexesStat.LastIndexedEtag))
								break;
						}
					});
				}
				catch (Exception e)
				{
					log.Error("Failed to execute task: " + task, e);
				}
				if (foundWork == false)
					context.WaitForWork(TimeSpan.FromSeconds(1));
			}
		}

		public bool IndexDocuments(IStorageActionsAccessor actions, string index, Guid etagToIndexFrom)
		{

			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
				return false; // index was deleted, probably

			var jsonDocs = actions.Documents.GetDocumentsAfter(etagToIndexFrom)
				.Where(x => x != null)
				.Take(10000) // ensure that we won't go overboard with reading and blow up with OOM
				.ToArray();

			if(jsonDocs.Length == 0)
				return false;
			
			try
			{
				log.DebugFormat("Indexing {0} documents for index: {1}", jsonDocs.Length, index);

				var failureRate = actions.Indexing.GetFailureRate(index);
				if (failureRate.IsInvalidIndex)
				{
					log.InfoFormat("Skipped indexing documents for index: {0} because failure rate is too high: {1}",
					                  index,
					                  failureRate.FailureRate);
					return false;
				}

				context.IndexStorage.Index(index, viewGenerator, jsonDocs.Select(x => JsonToExpando.Convert(x.ToJson())),
				                           context, actions);

				var last = jsonDocs.Last();
				actions.Indexing.UpdateLastIndexed(index, last.Etag, last.LastModified);
				return true;
			}
			catch (Exception e)
			{
				log.WarnFormat(e, "Failed to index documents for index: {0}", index);
				return false;
			}
			
		}
	}
}