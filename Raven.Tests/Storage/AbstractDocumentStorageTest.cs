using System.IO;

namespace Raven.Tests.Storage
{
	public class AbstractDocumentStorageTest : WithDebugging
	{
		public AbstractDocumentStorageTest()
		{
			if (Directory.Exists("raven.db.test.esent"))
				Directory.Delete("raven.db.test.esent", true);
		}
	}
}