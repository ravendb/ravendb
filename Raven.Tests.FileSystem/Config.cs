using Raven.Json.Linq;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Xunit;

namespace RavenFS.Tests
{
	public class Config : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task CanGetConfig_NotThere()
		{
			var client = NewAsyncClient();

            Assert.Null(await client.Configuration.GetKeyAsync<RavenJObject>("test"));
		}

		[Fact]
		public async Task CanSetConfig()
		{
			var client = NewAsyncClient();

            Assert.Null(await client.Configuration.GetKeyAsync<RavenJObject>("test"));

            await client.Configuration.SetKeyAsync("test", new RavenJObject
		                                            {
			                                            {"test", "there"},
			                                            {"hi", "you"}
		                                            });
            var nameValueCollection = await client.Configuration.GetKeyAsync<RavenJObject>("test");
			Assert.NotNull(nameValueCollection);

			Assert.Equal("there", nameValueCollection["test"]);
			Assert.Equal("you", nameValueCollection["hi"]);

		}


		[Fact]
		public async Task CanGetConfigNames()
		{
			var client = NewAsyncClient();

            Assert.Null(await client.Configuration.GetKeyAsync<RavenJObject>("test"));

            await client.Configuration.SetKeyAsync("test", new RavenJObject
		                                            {
			                                            {"test", "there"},
			                                            {"hi", "you"}
		                                            });

            await client.Configuration.SetKeyAsync("test2", new RavenJObject
				                                    {
					                                    {"test", "there"},
					                                    {"hi", "you"}
				                                    });
			var names = await client.Configuration.GetKeyNamesAsync();
			Assert.Equal(new[]{"Raven/Sequences/Raven/Etag", "test", "test2"}, names);
		}

		[Fact]
		public async Task CanDelConfig()
		{
			var client = NewAsyncClient();

            Assert.Null(await client.Configuration.GetKeyAsync<RavenJObject>("test"));

            await client.Configuration.SetKeyAsync("test", new RavenJObject
			                                        {
				                                        {"test", "there"},
				                                        {"hi", "you"}
			                                        });
            Assert.NotNull(await client.Configuration.GetKeyAsync<RavenJObject>("test"));

            await client.Configuration.DeleteKeyAsync("test");

            Assert.Null(await client.Configuration.GetKeyAsync<RavenJObject>("test"));
		}

	    [Fact]
	    public void CanGetTotalConfigCount()
	    {
	        var client = NewAsyncClient();

            client.Configuration.SetKeyAsync("TestConfigA", new RavenJObject()).Wait();
            client.Configuration.SetKeyAsync("TestConfigB", new RavenJObject()).Wait();

	        Assert.Equal(2, client.Configuration.SearchAsync(prefix: "Test").Result.TotalCount);
	    }

        [Fact]
        public void SearchResultsOnlyIncludeConfigsWithPrefix()
        {
            var client = NewAsyncClient();

            client.Configuration.SetKeyAsync("TestConfigA", new RavenJObject()).Wait();
            client.Configuration.SetKeyAsync("TestConfigB", new RavenJObject()).Wait();
            client.Configuration.SetKeyAsync("AnotherB", new RavenJObject()).Wait();

            Assert.Equal(2, client.Configuration.SearchAsync(prefix: "Test").Result.TotalCount);
        }
	}
}