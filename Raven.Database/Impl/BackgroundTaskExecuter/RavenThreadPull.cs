using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Impl.BackgroundTaskExecuter
{
	public class RavenThreadPull : IDisposable
	{
		public class ThreadTask
		{
			public BatchStatistics BatchStats;
			public DateTime QueuedAt;
			public Stopwatch Duration;
			public CountdownEvent DoneEvent;
			public object Description;
			public Action Action;
			public bool EarlyBreak;
		}

		public class ThreadData
		{
			public Thread Thread;
			public ManualResetEventSlim StopWork;
			public int Unstoppable;
		}

		private readonly CancellationToken _ct;

		public RavenThreadPull(int maxLevelOfParallelism, CancellationToken ct, string name = "RavenThreadPool")
		{
			_createLinkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(new[] { ct });
			_ct = _createLinkedTokenSource.Token;
			_threads = new ThreadData[maxLevelOfParallelism];
			for (int i = 0; i < _threads.Length; i++)
			{
				var copy = i;
				_threads[i] = new ThreadData
				{
					StopWork = new ManualResetEventSlim(false),
					Thread = new Thread(() => ExecutePoolWork(_threads[copy]))
					{
						Name = name + " " + i,
						IsBackground = true,
						Priority = ThreadPriority.Normal
					}
				};
			}
			_currentWorkingThreadsAmount = maxLevelOfParallelism;
		}

		public void Start()
		{
			foreach (var t in _threads)
			{
				t.Thread.Start();
			}
		}

		private readonly object _locker = new object();
		private ThreadData[] _threads;
		private readonly BlockingCollection<ThreadTask> _tasks = new BlockingCollection<ThreadTask>();
		private readonly AutoResetEvent _threadHasNoWorkToDo = new AutoResetEvent(false);

		private int _freeThreads;

		private readonly ConcurrentDictionary<ThreadTask, object> _runningTasks = new ConcurrentDictionary<ThreadTask, object>();
		[ThreadStatic]
		private static ThreadData _selfInformation;

		private int _currentWorkingThreadsAmount;

		public ThreadTask[] GetRunningTasks()
		{
			return _runningTasks.Keys.ToArray();
		}

		public ThreadTask[] GetAllWaitingTasks()
		{
			return _tasks.ToArray();
		}

		public int GetWaitingTasksAmount()
		{
			return _tasks.Count;
		}

		public int GetRunningTasksAmount()
		{
			return _runningTasks.Count;
		}

		public void ThrottleUp()
		{
			lock (_locker)
			{
				if (_threads == null)
					return;
				if (_currentWorkingThreadsAmount < _threads.Length)
				{
					_threads[_currentWorkingThreadsAmount].StopWork.Set();
					_currentWorkingThreadsAmount++;
					return;
				}

				foreach (var thread in _threads)
				{
					if (thread.Thread.Priority != ThreadPriority.BelowNormal)
						continue;

					thread.Thread.Priority = ThreadPriority.Normal;
					return;
				}

			}
		}

		public void ThrottleDown()
		{
			lock (_locker)
			{
				if (_threads == null)
					return;
				foreach (var thread in _threads)
				{
					if (thread.Thread.Priority != ThreadPriority.Normal)
						continue;

					thread.Thread.Priority = ThreadPriority.BelowNormal;
					return;
				}

				if (_currentWorkingThreadsAmount > 2)
				{
					_threads[_currentWorkingThreadsAmount - 1].StopWork.Reset();
					_currentWorkingThreadsAmount--;
				}
			}
		}

		private void ExecutePoolWork(ThreadData selfData)
		{
			bool setEarlyWork = false;
			while (_ct.IsCancellationRequested == false) // cancellation token
			{
				selfData.StopWork.Wait(_ct);
				_selfInformation = selfData;

				setEarlyWork = ExecuteWorkOnce(setEarlyWork);
			}
		}

		private readonly CancellationTokenSource _createLinkedTokenSource;

		private bool ExecuteWorkOnce(bool setEarlyBreak, bool blnIsPoolWork = true)
		{
			ThreadTask threadTask = null;
			if (!blnIsPoolWork && _tasks.TryTake(out threadTask) == false)
			{
				return true;
			}

			if (threadTask == null)
				threadTask = GetNextTask(setEarlyBreak);

			try
			{
				threadTask.Duration = Stopwatch.StartNew();
				try
				{
					_runningTasks.TryAdd(threadTask, null);
					threadTask.Action();
				}
				finally
				{
					if (threadTask.BatchStats != null)
						Interlocked.Increment(ref threadTask.BatchStats.Completed);
					threadTask.Duration.Stop();
					object _;
					_runningTasks.TryRemove(threadTask, out _);
				}
				return threadTask.EarlyBreak;
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				return false;
				//throw;
				// TODO: error handling, logging, etc
			}
		}




		private ThreadTask GetNextTask(bool setEarlyBreak)
		{
			ThreadTask threadTask;

			bool setFreeThread = false;
			while (true)
			{
				if (setEarlyBreak)
				{
					if (_tasks.TryTake(out threadTask, 1000))
						break;

					if (setFreeThread == false)
					{
						Interlocked.Increment(ref _freeThreads);
						setFreeThread = true;
					}
					_threadHasNoWorkToDo.Set();
					continue;
				}
				threadTask = _tasks.Take(_ct);
				break;
			}
			if (setFreeThread)
				Interlocked.Decrement(ref _freeThreads);
			return threadTask;
		}

		public class BatchStatistics
		{

			public int Completed;
			public int Total;
			public BatchType Type;

			public enum BatchType
			{
				Atomic,
				Range
			}
		}

		public class OperationDescription
		{
			public enum OpeartionType
			{
				Atomic,
				Range,
				Maintaenance
			}

			public OpeartionType Type;
			public string PlainText;
			public int From;
			public int To;
			public int Total;

			public override string ToString()
			{
				return string.Format("{0} range {1} to {2} of {3}", PlainText, From, To, Total);
			}
		}

		public void ExecuteBatch<T>(IList<T> src, Action<IEnumerator<T>> action, int pageSize = 1024, string description = null)
		{
			if (src.Count == 0)
				return;
			if (src.Count == 1)
			{
				using (var enumerator = src.GetEnumerator())
					action(enumerator);
				return;
			}
			var ranges = new ConcurrentQueue<Tuple<int, int>>();
			var now = DateTime.UtcNow;

			var totalTasks = new CountdownEvent(1);
			for (int i = 0; i < src.Count; i += pageSize)
			{
				var rangeStart = i;
				var rangeEnd = i + pageSize - 1 < src.Count ? i + pageSize - 1 : src.Count - 1;
				ranges.Enqueue(Tuple.Create(rangeStart, rangeEnd));

				if (i > 0)
					totalTasks.AddCount();

				var threadTask = new ThreadTask
				{
					Action = () =>
					{
						Tuple<int, int> range;
						if (ranges.TryDequeue(out range))
						{
							action(YieldFromRange(range, src));
							totalTasks.Signal();
						}
					},
					Description = new OperationDescription()
					{
						Type = OperationDescription.OpeartionType.Range,
						PlainText = description,
						From = i * pageSize,
						To = i * (pageSize + 1),
						Total = src.Count
					},
					QueuedAt = now,
					DoneEvent = totalTasks,
				};
				_tasks.Add(threadTask, _ct);
			}

			WaitForBatchToCompletion(totalTasks);
		}

		private IEnumerator<T> YieldFromRange<T>(Tuple<int, int> boundaries, IList<T> input)
		{
			for (int i = boundaries.Item1; i <= boundaries.Item2; i++)
			{
				yield return input[i];
			}
		}


		public void ExecuteBatch<T>(IList<T> src, Action<T> action, int pageSize = 1024, string description = null, bool allowPartialBatchResumption = false)
		{
			if (src.Count == 0)
				return;

			if (allowPartialBatchResumption == false && src.Count == 1)
			{
				action(src[0]);
				return;
			}

			var now = DateTime.UtcNow;
			CountdownEvent lastEvent = null;
			var itemsCount = 0;

			var batch = new BatchStatistics
			{
				Total = src.Count
			};

			lastEvent = new CountdownEvent(src.Count);

			for (; itemsCount < src.Count; itemsCount++)
			{
				var copy = itemsCount;
				var threadTask = new ThreadTask
				{
					Action = () =>
					{
						action(src[copy]);
						lastEvent.Signal();
					},
					Description = new OperationDescription()
					{
						Type = OperationDescription.OpeartionType.Atomic,
						PlainText = description,
						From = itemsCount + 1,
						To = itemsCount + 1,
						Total = src.Count,
					},
					BatchStats = batch,
					EarlyBreak = allowPartialBatchResumption,
					QueuedAt = now,
					DoneEvent = lastEvent,
				};
				_tasks.Add(threadTask, _ct);
			}

			if (allowPartialBatchResumption == false)
			{
				WaitForBatchToCompletion(lastEvent);
				return;
			}

			WaitForBatchAllowingPartialBatchResumption(lastEvent, batch);
		}

		private void WaitForBatchToCompletion(CountdownEvent completionEvent)
		{
			try
			{
				if (_selfInformation != null)
				{
					bool setEarlyBreak = false;
					do
					{
						setEarlyBreak = ExecuteWorkOnce(setEarlyBreak, false);
					} while (completionEvent.IsSet == false);
					return;
				}

				completionEvent.Wait(_ct);
			}
			finally
			{
				completionEvent.Dispose();
			}
		}

		private void WaitForBatchAllowingPartialBatchResumption(CountdownEvent completionEvent, BatchStatistics batch)
		{
			try
			{
				var waitHandles = new[] { completionEvent.WaitHandle, _threadHasNoWorkToDo };
				var sp = Stopwatch.StartNew();

				while (true)
				{
					var i = WaitHandle.WaitAny(waitHandles);
					if (i == 0)
						break;

					if (Thread.VolatileRead(ref batch.Completed) < batch.Total / 2)
						continue;

					var currentFreeThreads = Thread.VolatileRead(ref _freeThreads);
					if (currentFreeThreads < _currentWorkingThreadsAmount / 2)
					{
						break;
					}
				}

				var elapsedMilliseconds = (int)sp.ElapsedMilliseconds;
				if (completionEvent.Wait(Math.Max(elapsedMilliseconds / 2, 2500), _ct))
					return;

			}
			finally
			{
				_tasks.Add(new ThreadTask
				{
					Description = new OperationDescription()
					{
						Type = OperationDescription.OpeartionType.Maintaenance,
						PlainText = "Maintenance task"
					},
					Action = () =>
					{
						completionEvent.Wait();
						completionEvent.Dispose();
					}
				}, _ct);
			}


			// log that we are leaving batch
		}

		public void Dispose()
		{
			lock (_locker)
			{
				_createLinkedTokenSource.Cancel();
				for (int index = 0; index < _threads.Length; index++)
				{
					var t = _threads[index];
					t.StopWork.Set();
					t.Thread.Join();
					t.StopWork.Dispose();
				}
				_threads = null;
			}
		}
	}
}
