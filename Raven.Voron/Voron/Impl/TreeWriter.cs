namespace Voron.Impl
{
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Linq;
	using System.Threading;
	using System.Threading.Tasks;

	using Extensions;
	using Trees;

	public class TreeWriter
	{
		private readonly StorageEnvironment _env;

		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites;

		private readonly SemaphoreSlim _semaphore;

		internal TreeWriter(StorageEnvironment env)
		{
			_env = env;
			_pendingWrites = new ConcurrentQueue<OutstandingWrite>();
			_semaphore = new SemaphoreSlim(1, 1);
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			var mine = new OutstandingWrite(batch);
			_pendingWrites.Enqueue(mine);

			List<OutstandingWrite> writes = null;

			_semaphore.Wait();

			try
			{
				if (mine.Done)
					return;

				writes = BuildBatchGroup(mine);

				using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var g in writes.SelectMany(x => x.Batch.Operations).GroupBy(x=>x.TreeName))
					{
						var tree = GetTree(g.Key);
					    foreach (var operation in g)
					    {

                            switch (operation.Type)
                            {
                                case WriteBatch.BatchOperationType.Add:
                                    tree.Add(tx, operation.Key, operation.Value);
                                    break;
                                case WriteBatch.BatchOperationType.Delete:
                                    tree.Delete(tx, operation.Key);
                                    break;
                            }
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

		private Tree GetTree(string treeName)
		{
			if (treeName == _env.Root.Name) 
				return _env.Root;

			return _env.GetTree(treeName);
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