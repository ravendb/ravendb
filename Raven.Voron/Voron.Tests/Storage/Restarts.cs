using System;
using System.Diagnostics;
using System.IO;
using Voron.Debugging;
using Voron.Impl;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Restarts
    {

		protected void RenderAndShow(Transaction tx, int showEntries = 25, string name = null)
		{
			if (name == null)
				RenderAndShow(tx, tx.State.Root, showEntries);
			else
				RenderAndShow(tx, tx.GetTree(name), showEntries);
		}

		protected void RenderAndShow(Transaction tx, Tree root, int showEntries = 25)
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, "test-tree.dot");
			var rootPageNumber = tx.GetTree(root.Name).State.RootPageNumber;
			TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(rootPageNumber), showEntries);

			var output = Path.Combine(Environment.CurrentDirectory, "output.svg");
			var p = Process.Start(@"c:\Program Files (x86)\Graphviz2.30\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		}

        [Fact]
        public void DataIsKeptAfterRestart()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.GetInMemory())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.State.Root.Add(tx, "test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.State.Root.Add(tx, "test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
						RenderAndShow(tx, tx.State.Root, 1);

	                    if (tx.State.Root.Read(tx, "test/1") == null)
		                    Debugger.Launch();

                        Assert.NotNull(tx.State.Root.Read(tx, "test/1"));
                        Assert.NotNull(tx.State.Root.Read(tx, "test/2"));
                        tx.Commit();
                    }
                }
            }
        }

        [Fact]
        public void DataIsKeptAfterRestartForSubTrees()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.GetInMemory())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = tx.GetTree("test");
                        tree.Add(tx, "test", Stream.Null);
                        tx.Commit();

                        Assert.NotNull(tree.Read(tx, "test"));
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = env.CreateTree(tx, "test");
                        tx.Commit();
                    }

                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        var tree = tx.GetTree("test");
                        Assert.NotNull(tree.Read(tx, "test"));
                        tx.Commit();
                    }
                }
            }
        }
    }
}