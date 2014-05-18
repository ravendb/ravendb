using Raven.Json.Linq;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests
{
	public class Config : RavenFsTestBase
	{
		[Fact]
		public async Task CanGetConfig_NotThere()
		{
			var client = NewClient();

            Assert.Null(await client.Config.GetConfig<RavenJObject>("test"));
		}

		[Fact]
		public async Task CanSetConfig()
		{
			var client = NewClient();

            Assert.Null(await client.Config.GetConfig<RavenJObject>("test"));

            await client.Config.SetConfig("test", new RavenJObject
		                                            {
			                                            {"test", "there"},
			                                            {"hi", "you"}
		                                            });
            var nameValueCollection = await client.Config.GetConfig<RavenJObject>("test");
			Assert.NotNull(nameValueCollection);

			Assert.Equal("there", nameValueCollection["test"]);
			Assert.Equal("you", nameValueCollection["hi"]);

		}


		[Fact]
		public async Task CanGetConfigNames()
		{
			var client = NewClient();

            Assert.Null(await client.Config.GetConfig<RavenJObject>("test"));

            await client.Config.SetConfig("test", new RavenJObject
		                                            {
			                                            {"test", "there"},
			                                            {"hi", "you"}
		                                            });

            await client.Config.SetConfig("test2", new RavenJObject
				                                    {
					                                    {"test", "there"},
					                                    {"hi", "you"}
				                                    });
			var names = await client.Config.GetConfigNames();
			Assert.Equal(new[]{"Raven/Sequences/Raven/Etag", "test", "test2"}, names);
		}

		[Fact]
		public async Task CanDelConfig()
		{
			var client = NewClient();

            Assert.Null(await client.Config.GetConfig<RavenJObject>("test"));

            await client.Config.SetConfig("test", new RavenJObject
			                                        {
				                                        {"test", "there"},
				                                        {"hi", "you"}
			                                        });
            Assert.NotNull(await client.Config.GetConfig<RavenJObject>("test"));

			await client.Config.DeleteConfig("test");

            Assert.Null(await client.Config.GetConfig<RavenJObject>("test"));
		}

	    [Fact]
	    public void CanGetTotalConfigCount()
	    {
	        var client = NewClient();

            client.Config.SetConfig("TestConfigA", new RavenJObject()).Wait();
            client.Config.SetConfig("TestConfigB", new RavenJObject()).Wait();

	        Assert.Equal(2, client.Config.SearchAsync(prefix: "Test").Result.TotalCount);
	    }

        [Fact]
        public void SearchResultsOnlyIncludeConfigsWithPrefix()
        {
            var client = NewClient();

            client.Config.SetConfig("TestConfigA", new RavenJObject()).Wait();
            client.Config.SetConfig("TestConfigB", new RavenJObject()).Wait();
            client.Config.SetConfig("AnotherB", new RavenJObject()).Wait();

            Assert.Equal(2, client.Config.SearchAsync(prefix: "Test").Result.TotalCount);
        }
	}
}