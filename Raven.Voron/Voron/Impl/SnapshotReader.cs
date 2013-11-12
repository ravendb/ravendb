namespace Voron.Impl
{
	using System;
	using System.IO;

	using Voron.Trees;

	public class SnapshotReader : IDisposable
	{
		private readonly StorageEnvironment _env;

		public SnapshotReader(Transaction tx)
		{
			Transaction = tx;
			_env = Transaction.Environment;
		}

		public Transaction Transaction { get; private set; }

		public ReadResult Read(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
				ReadResult result;
				if (writeBatch.TryGetValue(treeName, key, out result, out operationType))
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							//when the returned ReadResult is disposed - prevent from stream currently in WriteBatch to be disposed as well
							return new ReadResult(CloneStream(result.Stream), (ushort)(result.Version + 1));
						case WriteBatch.BatchOperationType.Delete:
							return null;
					}
				}
			}

			var tree = GetTree(treeName);
			return tree.Read(Transaction, key);
		}

		public int GetDataSize(string treeName, Slice key)
		{
			var tree = GetTree(treeName);
			return tree.GetDataSize(Transaction, key);
		}

		//similar to read version, but ReadVersion returns 0 for items that are in WriteBatch
		public bool Contains(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
				ReadResult result;
				if (writeBatch.TryGetValue(treeName, key, out result, out operationType))
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							//when the returned ReadResult is disposed - prevent from stream currently in WriteBatch to be disposed as well
							return true;
						case WriteBatch.BatchOperationType.Delete:
							return false;
					}
				}
			}

			var tree = GetTree(treeName);
			return tree.ReadVersion(Transaction, key) > 0;

		}

		public ushort? ReadVersion(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
				ReadResult result;
				if (writeBatch.TryGetValue(treeName, key, out result, out operationType))
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							return (result.Version == 0) ? (ushort)0 : (ushort)(result.Version + 1);
						case WriteBatch.BatchOperationType.Delete:
							return 0;
					}
				}
			}

			var tree = GetTree(treeName);
			return tree.ReadVersion(Transaction, key);
		}

		public IIterator Iterate(string treeName, WriteBatch writeBatch = null)
		{
			var tree = GetTree(treeName);
			return tree.Iterate(Transaction, writeBatch);
		}

		public void Dispose()
		{
			Transaction.Dispose();
		}

		public IIterator MultiRead(string treeName, Slice key)
		{
			var tree = GetTree(treeName);
			return tree.MultiRead(Transaction, key);
		}

		private Tree GetTree(string treeName)
		{
			var tree = treeName == null ? Transaction.State.Root : Transaction.State.GetTree(treeName, Transaction);
			return tree;
		}	

		private static Stream CloneStream(Stream source)
		{
			var sourcePosition = source.Position;
			var destination = new MemoryStream();

			source.CopyTo(destination);
			destination.Position = sourcePosition;
			source.Position = sourcePosition;

			return destination;
		}
	}
}