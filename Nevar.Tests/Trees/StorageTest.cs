using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Tests.Trees
{
	public abstract class StorageTest : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;

		public StorageEnvironment Env
		{
			get { return _storageEnvironment; }
		}

		protected StorageTest()
		{
			_storageEnvironment = new StorageEnvironment(new PureMemoryPager());
		}

		protected Stream StreamFor(string val)
		{
			return new MemoryStream(Encoding.UTF8.GetBytes(val));
		}

		public void Dispose()
		{
			_storageEnvironment.Dispose();
		}

		protected void RenderAndShow(Transaction tx, int showEntries = 25)
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, "test-tree.dot");
			TreeDumper.Dump(tx, path, tx.GetCursor(Env.Root).Root, showEntries);

			var output = Path.Combine(Environment.CurrentDirectory, "output.svg");
			var p = Process.Start(@"C:\Users\Ayende\Downloads\graphviz-2.30.1\graphviz\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		}

		protected unsafe Tuple<Slice, Slice> ReadKey(Transaction tx, Slice key)
		{
			var cursor = tx.GetCursor(Env.Root);
			var p = Env.Root.FindPageFor(tx, key, cursor);
			var node = p.Search(key, Env.SliceComparer);

			if (node == null)
				return null;

			var item1 = new Slice(node);

			if (item1.Compare(key, Env.SliceComparer) != 0)
				return null;
			return Tuple.Create(item1,
								new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort)node->DataSize));
		}
	}
}