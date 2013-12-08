using System.Threading.Tasks;
using Voron.Util;

namespace Voron.Impl
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Linq;
	using System.Threading;
	using Extensions;

	public class TransactionMergingWriter : IDisposable
	{
		private readonly StorageEnvironment _env;

		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites;
		private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
		private readonly ManualResetEventSlim _hasWritesEvent = new ManualResetEventSlim();
		private readonly ManualResetEventSlim _stopWrites = new ManualResetEventSlim();

		private readonly ConcurrentQueue<ManualResetEventSlim> _eventsBuffer = new ConcurrentQueue<ManualResetEventSlim>();

		private readonly Lazy<Task> _backgroundTask;

		internal TransactionMergingWriter(StorageEnvironment env)
		{
			_env = env;
			_pendingWrites = new ConcurrentQueue<OutstandingWrite>();
			_stopWrites.Set();

			_backgroundTask = new Lazy<Task>(() => Task.Run(() => BackgroundWriter(), _cancellationTokenSource.Token));
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
				_hasWritesEvent.Set();

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
			var cancellationToken = _cancellationTokenSource.Token;
			while (cancellationToken.IsCancellationRequested == false)
			{
				_stopWrites.Wait(cancellationToken);
				_hasWritesEvent.Reset();

				OutstandingWrite write;
				while (_pendingWrites.TryPeek(out write))
				{
					HandleActualWrites(write);
				}

				_hasWritesEvent.Wait(cancellationToken);
			}
		}

		private void HandleActualWrites(OutstandingWrite mine)
		{
			List<OutstandingWrite> writes = null;
			try
			{
				writes = BuildBatchGroup(mine);
				using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
				{
					HandleOperations(tx, writes.SelectMany(x => x.Batch.Operations));

				    tx.Commit().ContinueWith(task =>
				    {
				        if (task.IsFaulted)
				        {
				            HandleWriteFailure(writes, task.Exception);
				        }
				        else
				        {
                            foreach (var write in writes)
                                write.Completed();
				        }
				    });
				}
			}
			catch (Exception e)
			{
				HandleWriteFailure(writes, e);
			}
			finally
			{
				Finalize(writes);
			}
		}

	    private void HandleWriteFailure(List<OutstandingWrite> writes, Exception e)
	    {
	        if (writes == null || writes.Count == 0)
	            throw new InvalidOperationException("Couldn't get items to write", e);

	        if (writes.Count == 1)
	        {
	            writes[0].Errorred(e);
	        }

	        SplitWrites(writes);
	    }

	    private void Finalize(IEnumerable<OutstandingWrite> writes)
		{
			if (writes == null)
				return;
			Debug.Assert(_pendingWrites.Count > 0);
			foreach (var write in writes)
			{
				Debug.Assert(_pendingWrites.Peek() == write);

				OutstandingWrite pendingWrite;
				_pendingWrites.TryDequeue(out pendingWrite);
			}
		}

		private void HandleOperations(Transaction tx, IEnumerable<WriteBatch.BatchOperation> operations)
		{
			foreach (var g in operations.GroupBy(x => x.TreeName))
			{
				var tree = tx.State.GetTree(g.Key, tx);
				// note that the ordering is done purely for performance reasons
				// we rely on the fact that there can be only a single operation per key in
				// each batch, and that we don't make any guarantees regarding ordering between
				// concurrent merged writes
				foreach (var operation in g.OrderBy(x => x.Key, SliceEqualityComparer.Instance))
				{
					operation.Reset();

					switch (operation.Type)
					{
						case WriteBatch.BatchOperationType.Add:
							tree.Add(tx, operation.Key, operation.Value as Stream, operation.Version);
							break;
						case WriteBatch.BatchOperationType.Delete:
							tree.Delete(tx, operation.Key, operation.Version);
							break;
						case WriteBatch.BatchOperationType.MultiAdd:
							tree.MultiAdd(tx, operation.Key, operation.Value as Slice, operation.Version);
							break;
						case WriteBatch.BatchOperationType.MultiDelete:
							tree.MultiDelete(tx, operation.Key, operation.Value as Slice, operation.Version);
							break;
						default:
							throw new ArgumentOutOfRangeException();
					}
				}
			}
		}

		private void SplitWrites(IEnumerable<OutstandingWrite> writes)
		{
			foreach (var write in writes)
			{
				try
				{
					using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
					{
						HandleOperations(tx, write.Batch.Operations);
						tx.Commit();

						write.Completed();
					}
				}
				catch (Exception e)
				{
					write.Errorred(e);
				}
			}
		}

		private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
		{
			// Allow the group to grow up to a maximum size, but if the
			// original write is small, limit the growth so we do not slow
			// down the small write too much.
			long maxSize = 16 * 1024 * 1024; // 16 MB by default
			if (mine.Size < 128 * 1024)
				maxSize = mine.Size + (1024 * 1024);

			var list = new List<OutstandingWrite>();
			var indexOfMine = -1;
			var index = 0;

			foreach (var write in _pendingWrites)
			{
				if (maxSize <= 0)
					break;

				if (write == mine)
				{
					indexOfMine = index;
					continue;
				}

				list.Add(write);

				maxSize -= write.Size;
				index++;
			}

			Debug.Assert(indexOfMine >= 0);

			list.Insert(indexOfMine, mine);

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
				Batch.Dispose();

				_transactionMergingWriter._eventsBuffer.Enqueue(_completed);
			}

			public void Errorred(Exception e)
			{
				_exception = e;
				_completed.Set();
			}

			public void Completed()
			{
				_completed.Set();
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
			_cancellationTokenSource.Cancel();
			_hasWritesEvent.Set();
			_stopWrites.Set();
			
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
		        if (e.InnerException is TaskCanceledException)
		            return;
		        throw;
		    }
		    finally
		    {
		        foreach (var manualResetEventSlim in _eventsBuffer)
		        {
		            manualResetEventSlim.Dispose();
		        }   
                _hasWritesEvent.Dispose();
                _stopWrites.Dispose();
		    }
		}
	}
}
