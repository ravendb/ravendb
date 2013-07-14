using System;
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
			var memoryMappedFile = MemoryMappedFile.CreateNew("test", 1024 * 1024, MemoryMappedFileAccess.ReadWrite);
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
	}
}