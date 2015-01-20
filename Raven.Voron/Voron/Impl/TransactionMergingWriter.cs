using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Voron.Debugging;
using Voron.Exceptions;
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

		private IEnumerable<WriteBatch.BatchOperation> GetBatchOperations(OutstandingWrite write)
		{
			var trees = write.Trees.ToList();

			var operations = new List<WriteBatch.BatchOperation>();
			trees.ForEach(tree => operations.AddRange(write.GetOperations(tree)));
			return operations.Where(x => x != null);
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

		[HandleProcessCorruptedStateExceptions]
		private void HandleActualWrites(OutstandingWrite mine, CancellationToken token)
		{
			List<OutstandingWrite> writes = null;
			try
			{
				writes = BuildBatchGroup(mine);
			    var completedSuccessfully = false;

				using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
				{
					HandleOperations(tx, writes, _cancellationToken);

					try
					{
						tx.Commit();
						if (ShouldRecordToDebugJournal)
							_debugJournal.Flush();

					    completedSuccessfully = true;
					}
					catch (Exception e)
					{
						if (e is SEHException)
						{
							e = new VoronUnrecoverableErrorException("Error occurred during write", new Win32Exception(error: e.HResult));
						}

						// if we have an error during the commit, we can't recover, just fail them all.
						foreach (var write in writes)
						{
							write.Errored(e);
						}
					}
				}

                if (completedSuccessfully)
                {
                    foreach (var write in writes)
                        write.Completed();
                }
			}
			catch (Exception e)
			{
				if (e is SEHException)
				{
					e = new VoronUnrecoverableErrorException("Error occurred during write", new Win32Exception(error: e.HResult));
				}

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

		private void HandleOperations(Transaction tx, List<OutstandingWrite> writes, CancellationToken token)
		{
			var trees = writes
				.SelectMany(x => x.Trees)
				.Distinct();

			foreach (var treeName in trees)
			{
				token.ThrowIfCancellationRequested();

				var tree = tx.State.GetTree(tx, treeName);
				foreach (var write in writes)
				{
					foreach (var operation in write.GetOperations(treeName))
					{
						token.ThrowIfCancellationRequested();

						operation.Reset();

						try
						{
							if (ShouldRecordToDebugJournal)
							{
								DebugActionType actionType;
								switch (operation.Type)
								{
									case WriteBatch.BatchOperationType.Add:
										actionType = DebugActionType.Add;
										break;
									case WriteBatch.BatchOperationType.Delete:
										actionType = DebugActionType.Delete;
										break;
									case WriteBatch.BatchOperationType.MultiAdd:
										actionType = DebugActionType.MultiAdd;
										break;
									case WriteBatch.BatchOperationType.MultiDelete:
										actionType = DebugActionType.MultiDelete;
										break;
									case WriteBatch.BatchOperationType.Increment:
										actionType = DebugActionType.Increment;
										break;
									case WriteBatch.BatchOperationType.AddStruct:
										actionType = DebugActionType.AddStruct; // TODO arek - debugjournal should handle structs
										break;
									default:
										throw new ArgumentOutOfRangeException();
								}

								_debugJournal.RecordWriteAction(actionType, tx, operation.Key, treeName, operation.GetValueForDebugJournal());
							}

							switch (operation.Type)
							{
								case WriteBatch.BatchOperationType.Add:
									if (operation.ValueStream != null)
										tree.Add(operation.Key, operation.ValueStream, operation.Version);
									else
										tree.Add(operation.Key, operation.ValueSlice, operation.Version);
									break;
								case WriteBatch.BatchOperationType.Delete:
									tree.Delete(operation.Key, operation.Version);
									break;
								case WriteBatch.BatchOperationType.MultiAdd:
									tree.MultiAdd(operation.Key, operation.ValueSlice, operation.Version);
									break;
								case WriteBatch.BatchOperationType.MultiDelete:
									tree.MultiDelete(operation.Key, operation.ValueSlice, operation.Version);
									break;
								case WriteBatch.BatchOperationType.Increment:
									tree.Increment(operation.Key, operation.ValueLong, operation.Version);
									break;
								case WriteBatch.BatchOperationType.AddStruct:
									tree.Write(operation.Key, operation.ValueStruct, operation.Version);
									break;
								default:
									throw new ArgumentOutOfRangeException();
							}
						}
						catch (Exception e)
						{
							if (operation.ExceptionTypesToIgnore.Contains(e.GetType()) == false)
								throw;
						}
					}
				}
			}
		}

		[HandleProcessCorruptedStateExceptions]
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
						HandleOperations(tx, new List<OutstandingWrite> { write }, _cancellationToken);
						tx.Commit();
						write.Completed();
					}
				}
				catch (Exception e)
				{
					if (e is SEHException)
					{
						e = new VoronUnrecoverableErrorException("Error occurred during write", new Win32Exception(error: e.HResult));
					}

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
			private readonly WriteBatch _batch;

			private readonly TransactionMergingWriter _transactionMergingWriter;
			private Exception _exception;
			private readonly ManualResetEventSlim _completed;

			private readonly Dictionary<string, List<WriteBatch.BatchOperation>> _operations = new Dictionary<string, List<WriteBatch.BatchOperation>>(); 

			public OutstandingWrite(WriteBatch batch, TransactionMergingWriter transactionMergingWriter)
			{
				_batch = batch;
				_transactionMergingWriter = transactionMergingWriter;

				_operations = CreateOperations(batch);
				Size = batch.Size();

				if (transactionMergingWriter._eventsBuffer.TryDequeue(out _completed) == false)
					_completed = new ManualResetEventSlim();
				_completed.Reset();
			}

			private static Dictionary<string, List<WriteBatch.BatchOperation>> CreateOperations(WriteBatch batch)
			{
				return batch.Trees.ToDictionary(tree => tree, tree => batch.GetSortedOperations(tree).ToList());
			}

			public IEnumerable<string> Trees
			{
				get
				{
					return _batch.Trees;
				}
			} 

			public IEnumerable<WriteBatch.BatchOperation> GetOperations(string treeName)
			{
				List<WriteBatch.BatchOperation> operations;
				if (_operations.TryGetValue(treeName, out operations))
					return operations;

				return Enumerable.Empty<WriteBatch.BatchOperation>();
			}

			public long Size { get; private set; }

			public void Dispose()
			{
				if (_batch.DisposeAfterWrite)
					_batch.Dispose();

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
