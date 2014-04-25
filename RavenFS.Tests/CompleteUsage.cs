using Raven.Json.Linq;
using System.Collections.Specialized;
using System.IO;
using Xunit;

namespace RavenFS.Tests
{
    public class CompleteUsage : RavenFsTestBase
	{
		[Fact]
		public void HowToUseTheClient()
		{
			var client = NewClient();
            var uploadTask = client.UploadAsync("dragon.design", new RavenJObject
			{
				{"Customer", "Northwind"},
				{"Preferred", "True"}
			}, new MemoryStream(new byte[] {1, 2, 3}));

			uploadTask.Wait(); // or we can just let it run

			var searchTask = client.SearchAsync("Customer:Northwind AND Preferred:True");

			searchTask.Wait();

			Assert.Equal("dragon.design", searchTask.Result.Files[0].Name);
		}
	}
}