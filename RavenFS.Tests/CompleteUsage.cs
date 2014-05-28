using Raven.Json.Linq;
using System.Collections.Specialized;
using System.IO;
using Xunit;

namespace RavenFS.Tests
{
    public class CompleteUsage : RavenFsTestBase
	{
		[Fact]
		public async void HowToUseTheClient()
		{
			var client = NewClient();
            var uploadTask = client.UploadAsync("dragon.design", new RavenJObject
			{
				{"Customer", "Northwind"},
				{"Preferred", "True"}
			}, new MemoryStream(new byte[] {1, 2, 3}));

			await uploadTask; // or we can just let it run

            var search = await client.SearchAsync("Customer:Northwind AND Preferred:True");

			Assert.Equal("dragon.design", search.Files[0].Name);
		}
	}
}