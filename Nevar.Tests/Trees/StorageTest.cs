using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;

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
			var memoryMappedFile = MemoryMappedFile.CreateNew("test", 1024 * 1024 *16, MemoryMappedFileAccess.ReadWrite);
			_storageEnvironment = new StorageEnvironment(memoryMappedFile);
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
	}
}