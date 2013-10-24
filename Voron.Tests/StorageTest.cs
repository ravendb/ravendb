using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Voron.Debugging;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Tests
{
	using System.Collections.Generic;

	public abstract class StorageTest : IDisposable
    {
        private readonly StorageEnvironment _storageEnvironment;
        private IVirtualPager _pager;

        public StorageEnvironment Env
        {
            get { return _storageEnvironment; }
        }

        protected StorageTest()
        {
            FilePager();
            //_pager = new PureMemoryPager();
            _storageEnvironment = new StorageEnvironment(_pager);
        }

        private void FilePager()
        {
            if (File.Exists("test.data"))
                File.Delete("test.data");
            _pager = new MemoryMapPager("test.data");
        }

        protected Stream StreamFor(string val)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(val));
        }

        public void Dispose()
        {
            _storageEnvironment.Dispose();
            _pager.Dispose();
            if (File.Exists("test.data"))
                File.Delete("test.data");
        }

        protected void RenderAndShow(Transaction tx, int showEntries = 25, string name = null)
        {
            if (name == null)
                RenderAndShow(tx, Env.RootTree(tx), showEntries);
            else
                RenderAndShow(tx, Env.GetTree(tx, name), showEntries);
        }

        protected void RenderAndShow(Transaction tx, Tree root, int showEntries = 25)
        {
            if (Debugger.IsAttached == false)
                return;
            var path = Path.Combine(Environment.CurrentDirectory, "test-tree.dot");
            var rootPageNumber = tx.GetTree(root.Name).State.RootPageNumber;
            TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(rootPageNumber), showEntries);

            var output = Path.Combine(Environment.CurrentDirectory, "output.svg");
            var p = Process.Start(@"c:\Program Files (x86)\Graphviz2.32\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
            p.WaitForExit();
            Process.Start(output);
        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction tx, Slice key)
        {
            using (var c = tx.NewCursor(Env.RootTree(tx)))
            {
                var p = Env.RootTree(tx).FindPageFor(tx, key, c);
                var node = p.Search(key, Env.SliceComparer);

                if (node == null)
                    return null;

                var item1 = new Slice(node);

                if (item1.Compare(key, Env.SliceComparer) != 0)
                    return null;
                return Tuple.Create(item1,
                                    new Slice((byte*) node + node->KeySize + Constants.NodeHeaderSize,
                                              (ushort) node->DataSize));
            }
        }

		protected IList<Tree> CreateTrees(StorageEnvironment env, int number, string prefix)
		{
			var results = new List<Tree>();

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < number; i++)
				{
					results.Add(env.CreateTree(tx, prefix + i));
				}

				tx.Commit();
			}

			return results;
		}
    }
}