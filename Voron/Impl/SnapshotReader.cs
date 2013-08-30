using System;
using System.IO;
using Voron.Trees;

namespace Voron.Impl
{
	public class SnapshotReader : IDisposable
	{
		private readonly Transaction _tx;
		private readonly StorageEnvironment _env;

		public SnapshotReader(Transaction tx)
		{
			_tx = tx;
			_env = _tx.Environment;
		}

		public ReadResult Read(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : _tx.Environment.GetTree(treeName);
			return tree.Read(_tx, key);
		}

		public ushort ReadVersion(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : _tx.Environment.GetTree(treeName);
			return tree.ReadVersion(_tx, key);
		}

		public TreeIterator Iterate(string treeName)
		{
			var tree = treeName == null ? _env.Root : _tx.Environment.GetTree(treeName);
			return tree.Iterate(_tx);
		}

		public void Dispose()
		{
			_tx.Dispose();
		}

		public IIterator MultiRead(string treeName, Slice key)
		{
			var tree = treeName == null ? _env.Root : _tx.Environment.GetTree(treeName);
			return tree.MultiRead(_tx, key);
		}
	}
}