namespace Voron.Tests.Bugs
{
	using System.IO;

	using Xunit;

	public class Disposing : StorageTest
	{
		[Fact]
		public void DisposingAndRecreatingStorageShouldWork()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}
		}
	}
}