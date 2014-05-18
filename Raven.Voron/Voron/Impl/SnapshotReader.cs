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
		    Tree tree = null;

			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
			    Stream stream;
			    ushort? version;
			    if (writeBatch.TryGetValue(treeName, key, out stream, out version, out operationType))
			    {
			        if (!version.HasValue) 
                        tree = GetTree(treeName);

					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
					    {
					        var reader = new ValueReader(stream);
					        return new ReadResult(reader, version.HasValue ? (ushort)(version.Value + 1) : tree.ReadVersion(Transaction, key));
					    }
						case WriteBatch.BatchOperationType.Delete:
							return null;
					}
				}
			}

		    if (tree == null) 
                tree = GetTree(treeName);

			return tree.Read(Transaction, key);
		}

		public int GetDataSize(string treeName, Slice key)
		{
			var tree = GetTree(treeName);
			return tree.GetDataSize(Transaction, key);
		}

		public bool Contains(string treeName, Slice key, out ushort? version, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
				Stream stream;
				if (writeBatch.TryGetValue(treeName, key, out stream, out version, out operationType))
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							return true;
						case WriteBatch.BatchOperationType.Delete:
							return false;
						default:
							throw new ArgumentOutOfRangeException(operationType.ToString());
					}
				}
			}

			var tree = GetTree(treeName);
			var readVersion = tree.ReadVersion(Transaction, key);

			var exists = readVersion > 0;

			version = exists ? (ushort?)readVersion : null;

			return exists;
		}

		public ushort ReadVersion(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				WriteBatch.BatchOperationType operationType;
			    Stream stream;
			    ushort? version;
			    if (writeBatch.TryGetValue(treeName, key, out stream, out version, out operationType) && version.HasValue)
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
						case WriteBatch.BatchOperationType.Delete:
					        return (ushort)(version.Value + 1);
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
			var tree = treeName == null ? Transaction.State.Root : Transaction.State.GetTree(Transaction, treeName);
			return tree;
		}	
	}
}