using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using System.Linq;
using Task = Raven.Database.Tasks.Task;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
{
	public abstract class AbstractIndexingExecuter
	{
		protected WorkContext context;
		protected TaskScheduler scheduler;
		protected static readonly ILog log = LogManager.GetCurrentClassLogger();
		protected ITransactionalStorage transactionalStorage;
		protected int workCounter;
		protected int lastFlushedWorkCounter;
		protected BaseBatchSizeAutoTuner autoTuner;

		protected AbstractIndexingExecuter(
			ITransactionalStorage transactionalStorage, WorkContext context, TaskScheduler scheduler)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
			this.scheduler = scheduler;
		}

		public void Execute()
		{
			var name = GetType().Name;
			var workComment = "WORK BY " + name;

			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = ExecuteIndexing();
					while (context.DoWork) // we want to drain all of the pending tasks before the next run
					{
						if (ExecuteTasks() == false)
							break;
						foundWork = true;
					}

				}
				catch (OutOfMemoryException oome)
				{
					foundWork = true;
					HandleOutOfMemoryException(oome);
				}
				catch (AggregateException ae)
				{
					foundWork = true;
					var oome = ae.ExtractSingleInnerException() as OutOfMemoryException;
					if (oome == null)
					{
						log.ErrorException("Failed to execute indexing", ae);
					}
					else
					{
						HandleOutOfMemoryException(oome);
					}
				}
				catch (OperationCanceledException)
				{
					log.Info("Got rude cancelation of indexing as a result of shutdown, aborting current indexing run");
					return;
				}
				catch (Exception e)
				{
					foundWork = true; // we want to keep on trying, anyway, not wait for the timeout or more work
					log.ErrorException("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeSpan.FromHours(1), ref workCounter, FlushIndexes, name);
				}
				else // notify the tasks executer that it has work to do
				{
					context.ShouldNotifyAboutWork(() => workComment);
					context.NotifyAboutWork();
				}
			}
		}

		private void HandleOutOfMemoryException(OutOfMemoryException oome)
		{
			log.WarnException(
				@"Failed to execute indexing because of an out of memory exception. Will force a full GC cycle and then become more conservative with regards to memory",
				oome);

			// On the face of it, this is stupid, because OOME will not be thrown if the GC could release
			// memory, but we are actually aware that during indexing, the GC couldn't find garbage to clean,
			// but in here, we are AFTER the index was done, so there is likely to be a lot of garbage.
			GC.Collect(GC.MaxGeneration);
			autoTuner.OutOfMemoryExceptionHappened();
		}

		private bool ExecuteTasks()
		{
			bool foundWork = false;
			transactionalStorage.Batch(actions =>
			{
				Task task = GetApplicableTask(actions);
				if (task == null)
					return;

				context.UpdateFoundWork();

				log.Debug("Executing {0}", task);
				foundWork = true;
				
				context.CancellationToken.ThrowIfCancellationRequested();

				try
				{
					task.Execute(context);
				}
				catch (Exception e)
				{
					log.WarnException(
						string.Format("Task {0} has failed and was deleted without completing any work", task),
						e);
				}
			});
			return foundWork;
		}

		protected abstract Task GetApplicableTask(IStorageActionsAccessor actions);

		private void FlushIndexes()
		{
			if (lastFlushedWorkCounter == workCounter || context.DoWork == false)
				return;
			lastFlushedWorkCounter = workCounter;
			FlushAllIndexes();
		}

		protected abstract void FlushAllIndexes();

		protected bool ExecuteIndexing()
		{
			var indexesToWorkOn = new List<IndexToWorkOn>();
			transactionalStorage.Batch(actions =>
			{
				foreach (var indexesStat in actions.Indexing.GetIndexesStats().Where(IsValidIndex))
				{
					var failureRate = actions.Indexing.GetFailureRate(indexesStat.Name);
					if (failureRate.IsInvalidIndex)
					{
						log.Info("Skipped indexing documents for index: {0} because failure rate is too high: {1}",
									   indexesStat.Name,
									   failureRate.FailureRate);
						continue;
					}
					if (IsIndexStale(indexesStat, actions) == false)
						continue;
					indexesToWorkOn.Add(GetIndexToWorkOn(indexesStat));
				}
			});

			if (indexesToWorkOn.Count == 0)
				return false;

			context.UpdateFoundWork();
			context.CancellationToken.ThrowIfCancellationRequested();

			using(context.IndexDefinitionStorage.CurrentlyIndexing())
				ExecuteIndexingWork(indexesToWorkOn);

			return true;
		}

		protected abstract IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat);

		protected abstract bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions);

		protected abstract void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn);

		protected abstract bool IsValidIndex(IndexStats indexesStat);
	}
}
