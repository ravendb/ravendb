namespace Voron.Impl
{
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Linq;
	using System.Threading;
	using System.Threading.Tasks;

	using Voron.Impl.Extensions;
	using Voron.Trees;

	public class TreeWriter
	{
		private readonly StorageEnvironment _env;

		private readonly Tree _tree;

		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites;

		private readonly SemaphoreSlim _semaphore;

		internal TreeWriter(StorageEnvironment env, Tree tree)
		{
			_env = env;
			_tree = tree;
			_pendingWrites = new ConcurrentQueue<OutstandingWrite>();
			_semaphore = new SemaphoreSlim(1, 1);
		}

		public Task WriteAsync(Slice key, Stream value)
		{
			var batch = new WriteBatch();
			batch.Add(key, value);

			return WriteAsync(batch);
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			var mine = new OutstandingWrite(batch);
			_pendingWrites.Enqueue(mine);

			List<OutstandingWrite> writes = null;

			await _semaphore.WaitAsync();

			try
			{
				if (mine.Done)
					return;

				writes = BuildBatchGroup(mine);

				using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var operation in writes.SelectMany(x => x.Batch.Operations))
					{
						switch (operation.Type)
						{
							case WriteBatch.BatchOperationType.Add:
								_tree.Add(tx, operation.Key, operation.Value);
								break;
							case WriteBatch.BatchOperationType.Delete:
								_tree.Delete(tx, operation.Key);
								break;
						}
					}

					tx.Commit();
				}
			}
			finally
			{
				if (writes != null)
				{
					foreach (var write in writes)
					{
						Debug.Assert(_pendingWrites.Peek() == write);

						OutstandingWrite pendingWrite;
						_pendingWrites.TryDequeue(out pendingWrite);
						pendingWrite.Done = true;
					}
				}

				_semaphore.Release();
			}
		}

		private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
		{
			// Allow the group to grow up to a maximum size, but if the
			// original write is small, limit the growth so we do not slow
			// down the small write too much.
			long maxSize = 1024 * 1024; // 1 MB by default
			if (mine.Size < 128 * 1024)
				maxSize = mine.Size + (128 * 1024);

			var list = new List<OutstandingWrite> { mine };

			foreach (var write in _pendingWrites)
			{
				if (maxSize <= 0)
					break;

				if (write == mine)
					continue;

				list.Add(write);

				maxSize -= write.Size;
			}

			return list;
		}

		private class OutstandingWrite
		{
			public OutstandingWrite(WriteBatch batch)
			{
				Batch = batch;
				Size = batch.Size;
			}

			public WriteBatch Batch { get; private set; }

			public long Size { get; private set; }

			public bool Done;
		}
	}
}