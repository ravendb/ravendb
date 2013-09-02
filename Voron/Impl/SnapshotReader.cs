using System;
using System.IO;
using Voron.Trees;

namespace Voron.Impl
{
	public class SnapshotReader : IDisposable
	{
		private readonly StorageEnvironment _env;

		public SnapshotReader(Transaction tx)
		{
			Transaction = tx;
			_env = Transaction.Environment;
		}

		public Transaction Transaction { get; private set; }

		public ReadResult Read(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : Transaction.Environment.GetTree(Transaction, treeName);
			return tree.Read(Transaction, key);
		}

		public ushort ReadVersion(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : Transaction.Environment.GetTree(Transaction, treeName);
			return tree.ReadVersion(Transaction, key);
		}

		public TreeIterator Iterate(string treeName)
		{
			var tree = treeName == null ? _env.Root : Transaction.Environment.GetTree(Transaction, treeName);
			return tree.Iterate(Transaction);
		}

		public void Dispose()
		{
			Transaction.Dispose();
		}

		public IIterator MultiRead(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : Transaction.Environment.GetTree(Transaction, treeName);
			return tree.MultiRead(Transaction, key);
		
		}
	}
}
