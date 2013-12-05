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

		private readonly ConditionVariableFactory _conditionVariableFactory = new ConditionVariableFactory();

		internal TransactionMergingWriter(StorageEnvironment env)
		{
			_env = env;
			_pendingWrites = new ConcurrentQueue<OutstandingWrite>();
		}

		public IDisposable PretendWriteStarted(int count)
		{
			_pendingWrites.Enqueue(new OutstandingWrite(new WriteBatch()));

			return new DisposableAction(() =>
			{
				OutstandingWrite write;
				_pendingWrites.TryDequeue(out write);
				SpinWait.SpinUntil(() => _pendingWrites.Count == count);
				while (_pendingWrites.Count == count)
				{
					foreach (var outstandingWrite in _pendingWrites)
					{
						outstandingWrite.ConditionVariable.Wake();
					}
				}
			});
		}

		public void Write(WriteBatch batch)
		{
			if (batch.Operations.Count == 0)
				return;

			using (var mine = new OutstandingWrite(batch)
			{
				ConditionVariable = _conditionVariableFactory.Create()
			})
			{
				_pendingWrites.Enqueue(mine);

				using (_conditionVariableFactory.EnterCriticalSection())
				{
					while (mine.Done() == false && _pendingWrites.Peek() != mine)
					{
						mine.ConditionVariable.Wait(-1);
					}

					if (mine.Done())
						return;

					HandleActualWrites(mine);
				}
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

					tx.Commit();
				}

				foreach (var write in writes)
					write.SetSuccess();
			}
			catch (Exception)
			{
				if (writes == null || writes.Count <= 1)
					throw;

				SplitWrites(writes);
			}
			finally
			{
				Finalize(writes);
			}

			Debug.Assert(mine.Status != OutstandingWriteStatus.Pending);
			mine.Done();
		}

		private void Finalize(IEnumerable<OutstandingWrite> writes)
		{
			if (writes != null)
			{
				foreach (var write in writes)
				{
					Debug.Assert(_pendingWrites.Peek() == write);

					OutstandingWrite pendingWrite;
					_pendingWrites.TryDequeue(out pendingWrite);
				}
			}
		}

		private void HandleOperations(Transaction tx, IEnumerable<WriteBatch.BatchOperation> operations)
		{
			foreach (var g in operations.GroupBy(x => x.TreeName))
			{
				var tree = tx.State.GetTree(g.Key, tx);
				foreach (var operation in g)
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

						write.SetSuccess();
					}
				}
				catch (Exception e)
				{
					write.SetError(e);
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
			private Exception exception;

			public OutstandingWrite(WriteBatch batch)
			{
				Batch = batch;
				Size = batch.Size();
				Status = OutstandingWriteStatus.Pending;
			}

			public WriteBatch Batch { get; private set; }

			public long Size { get; private set; }

			public OutstandingWriteStatus Status { get; private set; }
			public ConditionVariableFactory.ConditionVariable ConditionVariable { get; set; }

			public void SetSuccess()
			{
				Status = OutstandingWriteStatus.Success;
				exception = null;
				ConditionVariable.Wake();
			}

			public void SetError(Exception e)
			{
				Status = OutstandingWriteStatus.Error;
				exception = e;
				ConditionVariable.Wake();
			}

			public bool Done()
			{
				if (Status == OutstandingWriteStatus.Success)
					return true;

				if (Status == OutstandingWriteStatus.Pending)
					return false;

				throw exception;
			}

			public void Dispose()
			{
				Batch.Dispose();
				if (ConditionVariable != null)
					ConditionVariable.Dispose();
			}
		}

		private enum OutstandingWriteStatus
		{
			Pending,
			Success,
			Error
		}


		public void Dispose()
		{
			_conditionVariableFactory.Dispose();
		}
	}
}
