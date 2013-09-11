using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Client.Document.DTC
{
	public class VolatileOnlyTransactionRecoveryStorage : ITransactionRecoveryStorageContext, ITransactionRecoveryStorage
	{
		public void CreateFile(string name, Action<Stream> createFile)
		{
		}

		public void DeleteFile(string name)
		{
			// noop
		}

		public IEnumerable<string> GetFileNames(string filter)
		{
			yield break;
		}

		public Stream OpenRead(string name)
		{
			throw new NotSupportedException();
		}

		public void Dispose()
		{
			
		}

		public ITransactionRecoveryStorageContext Create()
		{
			return this;
		}
	}
}