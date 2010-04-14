using System;
using System.IO;

namespace Raven.Tests.Storage
{
	public class AbstractDocumentStorageTest : WithDebugging, IDisposable
	{
		public AbstractDocumentStorageTest()
		{
			if (Directory.Exists("raven.db.test.esent"))
				Directory.Delete("raven.db.test.esent", true);
		}

		public virtual void Dispose()
		{
			if (Directory.Exists("raven.db.test.esent"))
				Directory.Delete("raven.db.test.esent", true);
		}
	}
}