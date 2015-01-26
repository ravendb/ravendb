using Voron.Util;

namespace Voron.Impl
{
	using System;
	using System.IO;

	using Voron.Trees;

	public class SnapshotReader : IDisposable
	{

		public SnapshotReader(Transaction tx)
		{
			Transaction = tx;
		}

		public Transaction Transaction { get; private set; }

		public ReadResult Read(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			Tree tree = null;

			if (writeBatch != null && writeBatch.IsEmpty == false)
			{
				WriteBatch.BatchOperationType operationType;
				Stream stream;
				ValueType _;
				ushort? version;
				if (writeBatch.TryGetValue(treeName, key, out stream, out _, out version, out operationType))
				{
					if (!version.HasValue)
						tree = GetTree(treeName);

					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							{
								var reader = new ValueReader(stream);
								return new ReadResult(reader, version.HasValue ? (ushort)(version.Value + 1) : tree.ReadVersion(key));
							}
						case WriteBatch.BatchOperationType.Delete:
							return null;
					}
				}
			}

			if (tree == null)
				tree = GetTree(treeName);

			return tree.Read(key);
		}

		public StructReadResult<T> ReadStruct<T>(string treeName, Slice key, WriteBatch writeBatch = null) where T : struct 
		{
			Tree tree = null;

			if (writeBatch != null && writeBatch.IsEmpty == false)
			{
				WriteBatch.BatchOperationType operationType;
				ValueType value;
				Stream _;
				ushort? version;
				if (writeBatch.TryGetValue(treeName, key, out _, out value, out version, out operationType))
				{
					if (!version.HasValue)
						tree = GetTree(treeName);

					switch (operationType)
					{
						case WriteBatch.BatchOperationType.AddStruct:
							return new StructReadResult<T>((T) value, version.HasValue ? (ushort)(version.Value + 1) : tree.ReadVersion(key));
						case WriteBatch.BatchOperationType.Delete:
							return null;
					}
				}
			}

			if (tree == null)
				tree = GetTree(treeName);

			return tree.Read<T>(key);
		}

		public int GetDataSize(string treeName, Slice key)
		{
			var tree = GetTree(treeName);
			return tree.GetDataSize(key);
		}

		public bool Contains(string treeName, Slice key, out ushort? version, WriteBatch writeBatch = null)
		{
			if (writeBatch != null && writeBatch.IsEmpty == false)
			{
				WriteBatch.BatchOperationType operationType;
				Stream stream;
				ValueType valueType;
				if (writeBatch.TryGetValue(treeName, key, out stream, out valueType, out version, out operationType))
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
							return true;
						case WriteBatch.BatchOperationType.AddStruct:
							return true;
						case WriteBatch.BatchOperationType.Delete:
							return false;
						default:
							throw new ArgumentOutOfRangeException(operationType.ToString());
					}
				}
			}

			var tree = GetTree(treeName);
			var readVersion = tree.ReadVersion(key);

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
				ValueType valueType;
				ushort? version;
				if (writeBatch.TryGetValue(treeName, key, out stream, out valueType, out version, out operationType) && version.HasValue)
				{
					switch (operationType)
					{
						case WriteBatch.BatchOperationType.Add:
						case WriteBatch.BatchOperationType.AddStruct:
						case WriteBatch.BatchOperationType.Delete:
							return (ushort)(version.Value + 1);
					}
				}
			}

			var tree = GetTree(treeName);
			return tree.ReadVersion(key);
		}

		public IIterator Iterate(string treeName)
		{
			var tree = GetTree(treeName);
			return tree.Iterate();
		}

		public void Dispose()
		{
			Transaction.Dispose();
		}

		public IIterator MultiRead(string treeName, Slice key)
		{
			var tree = GetTree(treeName);
			return tree.MultiRead(key);
		}

		private Tree GetTree(string treeName)
		{
			var tree = treeName == null ? Transaction.State.Root : Transaction.State.GetTree(Transaction, treeName);
			return tree;
		}
	}
}