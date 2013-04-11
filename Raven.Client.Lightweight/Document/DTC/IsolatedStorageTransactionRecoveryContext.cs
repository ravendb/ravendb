#if !SILVERLIGHT && !NETFX_CORE

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using System.Threading;

namespace Raven.Client.Document.DTC
{
	public class IsolatedStorageTransactionRecoveryContext : ITransactionRecoveryStorageContext
	{
		private readonly IsolatedStorageFile machineStoreForApplication;

		public IsolatedStorageTransactionRecoveryContext()
		{
			machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForDomain();
		}

		public void CreateFile(string name, Action<Stream> createFile)
		{
			using (var file = machineStoreForApplication.CreateFile(name + ".temp"))
			{
				createFile(file);
				file.Flush(true);
			}
			machineStoreForApplication.MoveFile(name + ".temp", name);
		}

		public void DeleteFile(string name)
		{
			// docs says to retry: http://msdn.microsoft.com/en-us/library/system.io.isolatedstorage.isolatedstoragefile.deletefile%28v=vs.95%29.aspx
			int retries = 10;
			while (true)
			{
				if (machineStoreForApplication.FileExists(name) == false)
					break;
				try
				{
					machineStoreForApplication.DeleteFile(name);
					break;
				}
				catch (IsolatedStorageException)
				{
					retries -= 1;
					if (retries > 0)
					{
						Thread.Sleep(100);
						continue;
					}
					throw;
				}
			}
		}

		public IEnumerable<string> GetFileNames(string filter)
		{
			return machineStoreForApplication.GetFileNames(filter);
		}

		public Stream OpenRead(string name)
		{
			return machineStoreForApplication.OpenFile(name, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

		public void Dispose()
		{
			machineStoreForApplication.Dispose();
		}
	}
}
#endif
