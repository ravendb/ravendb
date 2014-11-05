using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Database.Server.RavenFS.Util
{
	public class AwaitableQueue<T>
	{
		Queue<T> queue = new Queue<T>();
		Queue<TaskCompletionSource<T>> waitingTasks = new Queue<TaskCompletionSource<T>>();
		object gate = new object();
		private bool completed;


		public bool TryEnqueue(T item)
		{
			lock (gate)
			{
				if (completed)
				{
					return false;
				}

				queue.Enqueue(item);
			}

			FulfillWaitingTasks();
			return true;
		}

		public bool TryDequeue(out T item)
		{
			lock (gate)
			{
				if (queue.Count > 0)
				{
					item = queue.Dequeue();
					return true;
				}

				item = default(T);
				return false;
			}
		}

		public Task<T> DequeueOrWaitAsync()
		{
			bool wasEnqueued = false;
			var tcs = new TaskCompletionSource<T>();
			lock (gate)
			{
				if (completed)
				{
					tcs.SetCanceled();
				}
				else
				{
					waitingTasks.Enqueue(tcs);
					wasEnqueued = true;
				}
			}

			if (wasEnqueued)
				FulfillWaitingTasks();

			return tcs.Task;
		}

		public void SignalCompletion()
		{
			var tasksToCancel = new List<TaskCompletionSource<T>>();

			lock (gate)
			{
				completed = true;
				while (waitingTasks.Count > 0)
				{
					tasksToCancel.Add(waitingTasks.Dequeue());
				}
			}

			foreach (var taskCompletionSource in tasksToCancel)
			{
				taskCompletionSource.TrySetCanceled();
			}
		}

		private void FulfillWaitingTasks()
		{
			for (var pair = GetNextItemAndTask(); pair.Item2 != null; pair = GetNextItemAndTask())
			{
				pair.Item2.TrySetResult(pair.Item1);
			}
		}

		private Tuple<T, TaskCompletionSource<T>> GetNextItemAndTask()
		{
			lock (gate)
			{
				if (queue.Count > 0 && waitingTasks.Count > 0)
					return Tuple.Create(queue.Dequeue(), waitingTasks.Dequeue());

				return Tuple.Create(default(T), default(TaskCompletionSource<T>));
			}
		}
	}
}
