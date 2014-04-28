using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Voron.Debugging;
using Voron.Util;

namespace Voron.Impl
{
	public class TransactionMergingWriter : IDisposable
	{
		private readonly StorageEnvironment _env;

		private readonly CancellationToken _cancellationToken;

		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites = new ConcurrentQueue<OutstandingWrite>();
		private readonly ManualResetEventSlim _stopWrites = new ManualResetEventSlim();
		private readonly ManualResetEventSlim _hasWrites = new ManualResetEventSlim();
		private readonly DebugJournal _debugJournal;
		private readonly ConcurrentQueue<ManualResetEventSlim> _eventsBuffer = new ConcurrentQueue<ManualResetEventSlim>();

		private bool ShouldRecordToDebugJournal
		{
			get
			{
				return _debugJournal != null && _debugJournal.IsRecording;
			}
		}

		private readonly Lazy<Task> _backgroundTask;

		internal TransactionMergingWriter(StorageEnvironment env, CancellationToken cancellationToken, DebugJournal debugJournal = null)
		{
			_env = env;
			_cancellationToken = cancellationToken;
			_stopWrites.Set();
			_debugJournal = debugJournal;
			_backgroundTask = new Lazy<Task>(() => Task.Factory.StartNew(BackgroundWriter, _cancellationToken,
																		 TaskCreationOptions.LongRunning,
																		 TaskScheduler.Current));
		}

		public IDisposable StopWrites()
		{
			_stopWrites.Reset();

			return new DisposableAction(() => _stopWrites.Set());
		}

		public void Write(WriteBatch batch)
		{
			if (batch.IsEmpty)
				return;

			EnsureValidBackgroundTaskState();

			using (var mine = new OutstandingWrite(batch, this))
			{
				_pendingWrites.Enqueue(mine);

				_hasWrites.Set();

				mine.Wait();
			}
		}

		private void EnsureValidBackgroundTaskState()
		{
			var backgroundTask = _backgroundTask.Value;
			if (backgroundTask.IsCanceled || backgroundTask.IsFaulted)
				backgroundTask.Wait(); // would throw
			if (backgroundTask.IsCompleted)
				throw new InvalidOperationException("The write background task has already completed!");
		}

		private void BackgroundWriter()
		{
			while (_cancellationToken.IsCancellationRequested == false)
			{
				_cancellationToken.ThrowIfCancellationRequested();

				_stopWrites.Wait(_cancellationToken);
				_hasWrites.Reset();

				OutstandingWrite write;
				while (_pendingWrites.TryDequeue(out write))
				{
					HandleActualWrites(write, _cancellationToken);
				}
				_hasWrites.Wait(_cancellationToken);
			}
		}

		private void HandleActualWrites(OutstandingWrite mine, CancellationToken token)
		{
			List<OutstandingWrite> writes = null;
			try
			{
				writes = BuildBatchGroup(mine);
				using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
				{
					HandleOperations(tx, writes.SelectMany(x => x.Batch.Operations), _cancellationToken);

					try
					{
						tx.Commit();
						if (ShouldRecordToDebugJournal)
							_debugJournal.Flush();

						foreach (var write in writes)
							write.Completed();
					}
					catch (Exception e)
					{
						// if we have an error duing the commit, we can't recover, just fail them all.
						foreach (var write in writes)
						{
							write.Errored(e);
						}
					}
				}
			}
			catch (Exception e)
			{
				HandleWriteFailure(writes, mine, e);
			}
		}

		private void HandleWriteFailure(List<OutstandingWrite> writes, OutstandingWrite mine, Exception e)
		{
			if (writes == null || writes.Count == 0)
			{
				mine.Errored(e);
				throw new InvalidOperationException("Couldn't get items to write", e);
			}

			if (writes.Count == 1)
			{
				writes[0].Errored(e);
				return;
			}

			SplitWrites(writes);
		}

		private void HandleOperations(Transaction tx, IEnumerable<WriteBatch.BatchOperation> operations, CancellationToken token)
		{
			foreach (var g in operations.GroupBy(x => x.TreeName))
			{
				token.ThrowIfCancellationRequested();

				var tree = tx.State.GetTree(tx, g.Key);
				// note that the ordering is done purely for performance reasons
				// we rely on the fact that there can be only a single operation per key in
				// each batch, and that we don't make any guarantees regarding ordering between
				// concurrent merged writes
				foreach (var operation in g.OrderBy(x => x))
				{
					token.ThrowIfCancellationRequested();

					operation.Reset();
					try
					{
						DebugActionType actionType;
						switch (operation.Type)
						{
							case WriteBatch.BatchOperationType.Add:
								var stream = operation.Value as Stream;
								if (stream != null)
									tree.Add(tx, operation.Key, stream, operation.Version);
								else
									tree.Add(tx, operation.Key, (Slice)operation.Value, operation.Version);
								actionType = DebugActionType.Add;
								break;
							case WriteBatch.BatchOperationType.Delete:
								tree.Delete(tx, operation.Key, operation.Version);
								actionType = DebugActionType.Delete;
								break;
							case WriteBatch.BatchOperationType.MultiAdd:
								tree.MultiAdd(tx, operation.Key, operation.Value as Slice, version: operation.Version);
								actionType = DebugActionType.MultiAdd;
								break;
							case WriteBatch.BatchOperationType.MultiDelete:
								tree.MultiDelete(tx, operation.Key, operation.Value as Slice, operation.Version);
								actionType = DebugActionType.MultiDelete;
								break;
							case WriteBatch.BatchOperationType.Increment:
								tree.Increment(tx, operation.Key, (long)operation.Value, operation.Version);
								actionType = DebugActionType.Increment;
								break;
							default:
								throw new ArgumentOutOfRangeException();
						}

						if (ShouldRecordToDebugJournal)
							_debugJournal.RecordAction(actionType, operation.Key, g.Key, operation.Value);
					}
					catch (Exception e)
					{
						if (operation.ExceptionTypesToIgnore.Contains(e.GetType()) == false)
							throw;
					}
				}
			}
		}

		private void SplitWrites(List<OutstandingWrite> writes)
		{
			for (var index = 0; index < writes.Count; index++)
			{
				var write = writes[index];
				try
				{
					_cancellationToken.ThrowIfCancellationRequested();

					using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
					{
						HandleOperations(tx, write.Batch.Operations, _cancellationToken);
						tx.Commit();
						write.Completed();
					}
				}
				catch (Exception e)
				{
					write.Errored(e);
				}
			}
		}

		private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
		{
			// Allow the group to grow up to a maximum size, but if the
			// original write is small, limit the growth so we do not slow
			// down the small write too much.
			long maxSize = 64 * 1024 * 1024; // 64 MB by default
			if (mine.Size < 128 * 1024)
				maxSize = (2 * 1024 * 1024); // 2 MB if small

			var list = new List<OutstandingWrite> { mine };

			maxSize -= mine.Size;

			while (true)
			{
				if (maxSize <= 0)
					break;

				OutstandingWrite item;
				if (_pendingWrites.TryDequeue(out item) == false)
					break;
				list.Add(item);
				maxSize -= item.Size;
			}

			return list;
		}

		private class OutstandingWrite : IDisposable
		{
			private readonly TransactionMergingWriter _transactionMergingWriter;
			private Exception _exception;
			private readonly ManualResetEventSlim _completed;

			public OutstandingWrite(WriteBatch batch, TransactionMergingWriter transactionMergingWriter)
			{
				_transactionMergingWriter = transactionMergingWriter;
				Batch = batch;
				Size = batch.Size();

				if (transactionMergingWriter._eventsBuffer.TryDequeue(out _completed) == false)
					_completed = new ManualResetEventSlim();
				_completed.Reset();
			}

			public WriteBatch Batch { get; private set; }

			public long Size { get; private set; }

			public void Dispose()
			{
				if (Batch.DisposeAfterWrite)
					Batch.Dispose();

				_transactionMergingWriter._eventsBuffer.Enqueue(_completed);
			}

			public void Errored(Exception e)
			{
				var wasSet = _completed.IsSet;

				_exception = e;
				_completed.Set();

				if (wasSet)
					throw new InvalidOperationException("This should not happen.");
			}

			public void Completed()
			{
				var wasSet = _completed.IsSet;
				_completed.Set();

				if (wasSet)
					throw new InvalidOperationException("This should not happen.");
			}

			public void Wait()
			{
				_completed.Wait();
				if (_exception != null)
				{
					throw new AggregateException("Error when executing write", _exception);
				}
			}
		}

		public void Dispose()
		{
			_stopWrites.Set();
			_hasWrites.Set();

			try
			{
				if (_backgroundTask.IsValueCreated == false)
					return;
				_backgroundTask.Value.Wait();
			}
			catch (TaskCanceledException)
			{
			}
			catch (AggregateException e)
			{
				if (e.InnerException is TaskCanceledException || e.InnerException is OperationCanceledException)
					return;
				throw;
			}
			finally
			{
				foreach (var manualResetEventSlim in _eventsBuffer)
				{
					manualResetEventSlim.Dispose();
				}
				_stopWrites.Dispose();
				_hasWrites.Dispose();
			}
		}
	}
}
