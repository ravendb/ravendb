using System.IO;

namespace Raven.Storage.Tests
{
	public class TxStorageTest
	{
		public TxStorageTest()
		{
			if(Directory.Exists("test"))
				Directory.Delete("test", true);
		}
	}
}