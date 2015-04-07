using Voron.Util;

namespace Voron.Impl
{
    using System;
    using System.IO;
    using System.Runtime.CompilerServices;
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
				WriteBatch.InBatchValue result;
				if (writeBatch.TryGetValue(treeName, key, out result))
				{
					if (!result.Version.HasValue)
						tree = GetTree(treeName);

					switch (result.OperationType)
					{
						case WriteBatch.BatchOperationType.Add:
						{
							var reader = new ValueReader(result.Stream);
							return new ReadResult(reader, result.Version.HasValue ? (ushort) (result.Version.Value + 1) : tree.ReadVersion(key));
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

		public StructReadResult<T> ReadStruct<T>(string treeName, Slice key, StructureSchema<T> schema, WriteBatch writeBatch = null)
		{

			Tree tree = null;

			if (writeBatch != null && writeBatch.IsEmpty == false)
			{
				WriteBatch.InBatchValue result;
				if (writeBatch.TryGetValue(treeName, key, out result))
				{
					if (!result.Version.HasValue)
						tree = GetTree(treeName);

					switch (result.OperationType)
					{
						case WriteBatch.BatchOperationType.AddStruct:
							return new StructReadResult<T>(new StructureReader<T>((Structure<T>) result.Struct, schema),  result.Version.HasValue ? (ushort)(result.Version.Value + 1) : tree.ReadVersion(key));
						case WriteBatch.BatchOperationType.Delete:
							return null;
					}
				}
			}

			if (tree == null)
				tree = GetTree(treeName);

			return tree.ReadStruct(key, schema);
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
				WriteBatch.InBatchValue result;
				if (writeBatch.TryGetValue(treeName, key, out result))
				{
					version = result.Version;

					switch (result.OperationType)
					{
						case WriteBatch.BatchOperationType.Add:
							return true;
						case WriteBatch.BatchOperationType.AddStruct:
							return true;
						case WriteBatch.BatchOperationType.Delete:
							return false;
						default:
							throw new ArgumentOutOfRangeException(result.OperationType.ToString());
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
				WriteBatch.InBatchValue result;
				if (writeBatch.TryGetValue(treeName, key, out result) && result.Version.HasValue)
				{
					switch (result.OperationType)
					{
						case WriteBatch.BatchOperationType.Add:
						case WriteBatch.BatchOperationType.AddStruct:
						case WriteBatch.BatchOperationType.Delete:
							return (ushort)(result.Version.Value + 1);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
		private Tree GetTree(string treeName)
		{
			var tree = treeName == null ? Transaction.State.Root : Transaction.State.GetTree(Transaction, treeName);
			return tree;
		}
	}
}