using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Tests
{
	using System.Collections.Generic;

	public abstract class StorageTest : IDisposable
	{
		private StorageEnvironment _storageEnvironment;
		protected StorageEnvironmentOptions _options;

		public StorageEnvironment Env
		{
		    get
		    {
			    if (_storageEnvironment == null)
			    {
				    lock (this)
				    {
					    if (_storageEnvironment == null)
						    _storageEnvironment = new StorageEnvironment(_options);
				    }
			    }
		        return _storageEnvironment;
		    }
		}

		protected StorageTest()
		{
			DeleteDirectory("test.data");
			_options = StorageEnvironmentOptions.ForPath("test.data");
			Configure(_options);
		}

		protected void RestartDatabase()
		{
			StopDatabase();

			StartDatabase();
		}

		protected void StartDatabase()
		{
			_storageEnvironment = new StorageEnvironment(_options);
		}

		protected void StopDatabase()
		{
			var ownsPagers = _options.OwnsPagers;
			_options.OwnsPagers = false;

			_storageEnvironment.Dispose();

			_options.OwnsPagers = ownsPagers;
		}

		protected void DeleteDirectory(string dir)
		{
			if (Directory.Exists(dir) == false)
				return;

			for (int i = 0; i < 10; i++)
			{
				try
				{
					Directory.Delete(dir, true);
					return;
				}
				catch (DirectoryNotFoundException)
				{
					return;
				}
				catch (Exception)
				{
					Thread.Sleep(13);
				}
			}

			Directory.Delete(dir, true);
		}

		protected virtual void Configure(StorageEnvironmentOptions options)
		{

		}

		protected Stream StreamFor(string val)
		{
			return new MemoryStream(Encoding.UTF8.GetBytes(val));
		}

		public virtual void Dispose()
		{
		    if (_storageEnvironment != null)
		        _storageEnvironment.Dispose();
			_options.Dispose();
			DeleteDirectory("test.data");

			_storageEnvironment = null;
			_options = null;
			GC.Collect(GC.MaxGeneration);
			GC.WaitForPendingFinalizers();
		}

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
			var p = Process.Start(FindGraphviz() + @"\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		}

	    private string FindGraphviz()
	    {
            var path = @"C:\Program Files (x86)\Graphviz2.";
	        for (var i = 0; i < 100; i++)
	        {
	            var p = path + i.ToString("00");

	            if (Directory.Exists(p)) 
                    return p;
	        }

            throw new InvalidOperationException("No Graphviz found.");
	    }

		protected unsafe Tuple<Slice, Slice> ReadKey(Transaction tx, Slice key)
		{
			using (var c = tx.NewCursor(tx.State.Root))
			{
				var p = tx.State.Root.FindPageFor(tx, key, c);
				var node = p.Search(key, Env.SliceComparer);

				if (node == null)
					return null;

				var item1 = new Slice(node);

				if (item1.Compare(key, Env.SliceComparer) != 0)
					return null;
				return Tuple.Create(item1,
									new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize,
											  (ushort)node->DataSize));
			}
		}

		protected IList<string> CreateTrees(StorageEnvironment env, int number, string prefix)
		{
			var results = new List<string>();

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < number; i++)
				{
					results.Add(env.CreateTree(tx, prefix + i).Name);
				}

				tx.Commit();
			}

			return results;
		}
	}
}