using System.IO;

namespace Voron.Impl
{
	using System;
	using Trees;

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
				var addedValues = writeBatch.GetAddedValues(treeName);
				var deletedValues = writeBatch.GetDeletedValues(treeName);

				if (deletedValues.ContainsKey(key))
					return null;

				if (addedValues.ContainsKey(key))
				{
					var relevantReadResult = addedValues[key];

					//when the returned ReadResult is disposed - prevent from stream currently in WriteBatch to be disposed as well
					return new ReadResult(CloneStream(relevantReadResult.Stream), (ushort)(relevantReadResult.Version + 1));
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
				var addedValues = writeBatch.GetAddedValues(treeName);
				var deletedValues = writeBatch.GetDeletedValues(treeName);

				if (deletedValues.ContainsKey(key))
					return false;

				if (addedValues.ContainsKey(key))
					return true;
			}

			var tree = GetTree(treeName);
			return tree.ReadVersion(Transaction, key) > 0;

		}

		public ushort ReadVersion(string treeName, Slice key, WriteBatch writeBatch = null)
		{
			if (writeBatch != null)
			{
				var addedValues = writeBatch.GetAddedValues(treeName);
				var deletedValues = writeBatch.GetDeletedValues(treeName);

				if (deletedValues.ContainsKey(key))
					return 0;

				if (addedValues.ContainsKey(key))
					return (addedValues[key].Version == 0) ? (ushort)0 : (ushort)(addedValues[key].Version + 1);
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
			var tree = treeName == null ? _env.Root : Transaction.Environment.GetTree(Transaction, treeName);
			return tree;
		}

		private static Stream CloneStream(Stream source)
		{
			var destination = new MemoryStream();
			source.CopyTo(destination);
			destination.Position = 0;

			return destination;
		}
	}
}