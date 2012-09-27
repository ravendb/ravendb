using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Permissions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Config;

namespace Raven.Database.Indexing
{
	[DebuggerDisplay("Id={Id}")]
	[PermissionSet(SecurityAction.InheritanceDemand, Unrestricted = true)]
	[HostProtection(SecurityAction.LinkDemand, Synchronization = true, ExternalThreading = true)]
	public class TaskSchedulerWithCustomPriority : TaskScheduler, IDisposable
	{
		private readonly ThreadPriority _threadPriority;
		private readonly List<Thread> _threads = new List<Thread>();
		private BlockingCollection<Task> _tasks = new BlockingCollection<Task>();

		public TaskSchedulerWithCustomPriority(int maxThreads, ThreadPriority threadPriority)
		{
			if (maxThreads < 1) throw new ArgumentOutOfRangeException("maxThreads");

			_threadPriority = threadPriority;
			for (int i = 0; i < maxThreads; i++)
			{
				CreateThread();
			}
		}

		private void CreateThread()
		{
			var thread = new Thread(() =>
			{
				foreach (var task in _tasks.GetConsumingEnumerable())
				{
					TryExecuteTask(task);
				}
			})
			{
				IsBackground = true, 
				Priority = _threadPriority
			};
			_threads.Add(thread);
			thread.Start();
		}

		protected override void QueueTask(Task task)
		{
			_tasks.Add(task);
		}

		protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
		{
			// ApartnentStates must match
			if (Thread.CurrentThread.GetApartmentState() != _threads[0].GetApartmentState())
				return false;

			// Thread priority must match
			if (Thread.CurrentThread.Priority != _threadPriority)
				return false;

			return TryExecuteTask(task);
		}

		protected override IEnumerable<Task> GetScheduledTasks()
		{
			return _tasks.ToArray();
		}

		public override int MaximumConcurrencyLevel
		{
			get
			{
				if(MemoryStatistics.MaxParallelismSet)
				{
					return Math.Min(MemoryStatistics.MaxParallelism, _threads.Count);
				}

				return _threads.Count;
			}
		}

		public void Dispose()
		{
			if (_tasks == null) return;
			
			_tasks.CompleteAdding();
			foreach (var thread in _threads) thread.Join();
			_tasks.Dispose();
			_tasks = null;
		}
	}
}
