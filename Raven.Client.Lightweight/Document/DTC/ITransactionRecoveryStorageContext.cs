using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Client.Document.DTC
{
	public interface ITransactionRecoveryStorageContext : IDisposable
	{
		void CreateFile(string name, Action<Stream> createFile);
		void DeleteFile(string name);
		IEnumerable<string> GetFileNames(string filter);
		Stream OpenRead(string name);
	}
}