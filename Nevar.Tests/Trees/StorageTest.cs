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

		protected void RenderAndShow(Transaction tx,Tree tree)
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.GetTempFileName();
			TreeDumper.Dump(tx, path, tree.Root, showNodesEvery:1);

			var output = Path.Combine(Environment.CurrentDirectory, "output.png");
			var p = Process.Start(@"C:\Users\Ayende\Downloads\graphviz-2.30.1\graphviz\bin\dot.exe", "-Tpng  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		}
	}
}