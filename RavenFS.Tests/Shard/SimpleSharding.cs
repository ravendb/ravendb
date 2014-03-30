using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Shard;
using Xunit;

namespace RavenFS.Tests.Shard
{
    public class SimpleSharding : RavenFsTestBase
	{
	    readonly ShardedRavenFileSystemClient shardedClient;

		public SimpleSharding()
		{
			var client1 = NewClient(0, fileSystemName: "shard1");
			var client2 = NewClient(1, fileSystemName: "shard2");
			shardedClient = new ShardedRavenFileSystemClient(new ShardStrategy(new Dictionary<string, RavenFileSystemClient>
				{
					{"1", client1},
					{"2", client2},
				}));
		}

		[Fact]
		public void CanGetSharding()
		{
			var shards = shardedClient.GetShardsToOperateOn(new ShardRequestData{Keys = new List<string>{"test.bin"}});
			Assert.Equal(shards.Count, 2);
		}

		[Fact]
		public async Task CanGetFileFromSharding()
		{       
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var newFileName = await shardedClient.UploadAsync("abc.txt", ms);

            var ms2 = new MemoryStream();
            await shardedClient.DownloadAsync(newFileName, ms2);

            ms2.Position = 0;

            var actual = new StreamReader(ms2).ReadToEnd();
            Assert.Equal(expected, actual);
		}

	    [Fact]
	    public async Task CanBrowseWithSharding()
	    {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            await shardedClient.UploadAsync("a.txt", ms);
            await shardedClient.UploadAsync("b.txt", ms);
            await shardedClient.UploadAsync("c.txt", ms);
            await shardedClient.UploadAsync("d.txt", ms);
            await shardedClient.UploadAsync("e.txt", ms);

	        var pagingInfo = new PagingInfo(shardedClient.NumberOfShards);
	        var result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(2, result.Length);

	        pagingInfo.CurrentPage++;
            result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(2, result.Length);

            pagingInfo.CurrentPage++;
            result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(1, result.Length);

            pagingInfo.CurrentPage++;
            result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(0, result.Length);
	    }

        [Fact]
        public async Task CanBrowseToAdvancedPageWithSharding()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            await shardedClient.UploadAsync("a.txt", ms);
            await shardedClient.UploadAsync("b.txt", ms);
            await shardedClient.UploadAsync("c.txt", ms);
            await shardedClient.UploadAsync("d.txt", ms);
            await shardedClient.UploadAsync("e.txt", ms);

            var pagingInfo = new PagingInfo(shardedClient.NumberOfShards){CurrentPage = 2};
            var result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(1, result.Length);

            pagingInfo.CurrentPage++;
            result = await shardedClient.BrowseAsync(2, pagingInfo);
            Assert.Equal(0, result.Length);

        }

        [Fact]
        public async Task CanNotBrowseToPageFarAway()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            await shardedClient.UploadAsync("a.txt", ms);
            await shardedClient.UploadAsync("b.txt", ms);
            await shardedClient.UploadAsync("c.txt", ms);
            await shardedClient.UploadAsync("d.txt", ms);
            await shardedClient.UploadAsync("e.txt", ms);

            var pagingInfo = new PagingInfo(shardedClient.NumberOfShards) { CurrentPage = 20 };
            try
            {
                await shardedClient.BrowseAsync(2, pagingInfo);
                Assert.Equal(true, false);//Should not get here
            }
            catch (Exception exception)
            {
                Assert.IsType<InvalidOperationException>(exception);
            }
        }

		private Stream StreamOfLength(int length)
		{
			var memoryStream = new MemoryStream(Enumerable.Range(0, length).Select(i => (byte)i).ToArray());

			return memoryStream;
		}

		[Fact]
		public async Task CanSearchForFilesBySizeWithSharding()
		{
			var name1 = await shardedClient.UploadAsync("1", StreamOfLength(1));
			var name2 = await shardedClient.UploadAsync("2", StreamOfLength(2));
			var name3 = await shardedClient.UploadAsync("3", StreamOfLength(3));
			var name4 = await shardedClient.UploadAsync("4", StreamOfLength(4));
			var name5 = await shardedClient.UploadAsync("5", StreamOfLength(5));

			var result = await shardedClient.SearchAsync("__size_numeric:[2 TO 4]");
			var files = result.Files;
			var fileNames = files.Select(f => f.Name).ToArray();

			Assert.Equal(3, result.FileCount);
			Assert.Contains(name2, fileNames);
			Assert.Contains(name3, fileNames);
			Assert.Contains(name4, fileNames);
		}

		[Fact]
		public async Task CanSearchForFilesBySizeWithShardingWithFields()
		{
			var name1 = await shardedClient.UploadAsync("111", StreamOfLength(100));
			var name2 = await shardedClient.UploadAsync("2", StreamOfLength(2));
			var name3 = await shardedClient.UploadAsync("33", StreamOfLength(3));
			var name4 = await shardedClient.UploadAsync("4", StreamOfLength(4));
			var name5 = await shardedClient.UploadAsync("55555", StreamOfLength(5));

			var result = await shardedClient.SearchAsync("", new []{"__size"});
			var files = result.Files;
			var fileNames = files.Select(f => f.Name).ToArray();

			Assert.Equal(new[] {name2, name3, name4, name5, name1 }, fileNames);
		}

		[Fact]
		public async Task CanSearchForFilesBySizeWithShardingWithFieldsDecending()
		{
			var name1 = await shardedClient.UploadAsync("111", StreamOfLength(100));
			var name2 = await shardedClient.UploadAsync("2", StreamOfLength(2));
			var name3 = await shardedClient.UploadAsync("33", StreamOfLength(3));
			var name4 = await shardedClient.UploadAsync("4", StreamOfLength(4));
			var name5 = await shardedClient.UploadAsync("55555", StreamOfLength(5));

			var result = await shardedClient.SearchAsync("", new[] { "-__size" });
			var files = result.Files;
			var fileNames = files.Select(f => f.Name).ToArray();

			Assert.Equal(new[] { name2, name3, name4, name5, name1 }.Reverse(), fileNames);
		}

		[Fact]
		public async Task CanSearchForFilesByMetadataWithShardingWithFields()
		{
			await shardedClient.UploadAsync("111", new NameValueCollection {{"Active", "true"}}, StreamOfLength(100));
			await shardedClient.UploadAsync("2", new NameValueCollection { { "Active", "false" } }, StreamOfLength(2));
			await shardedClient.UploadAsync("33", new NameValueCollection { { "Active", "false" } }, StreamOfLength(3));
			await shardedClient.UploadAsync("4", new NameValueCollection { { "Active", "false" } }, StreamOfLength(4));
			await shardedClient.UploadAsync("55555", new NameValueCollection { { "Active", "true" } }, StreamOfLength(5));

			var result = await shardedClient.SearchAsync("", new[] { "Active" });
			var files = result.Files;

			Assert.Equal("false", files[0].Metadata["Active"]);
			Assert.Equal("false", files[1].Metadata["Active"]);
			Assert.Equal("false", files[2].Metadata["Active"]);
			Assert.Equal("true", files[3].Metadata["Active"]);
			Assert.Equal("true", files[4].Metadata["Active"]);
		}

		[Fact]
		public async Task CanSearchForFilesByMetadataWithShardingWithFieldsDecending()
		{
			await shardedClient.UploadAsync("111", new NameValueCollection { { "Active", "true" } }, StreamOfLength(100));
			await shardedClient.UploadAsync("2", new NameValueCollection { { "Active", "false" } }, StreamOfLength(2));
			await shardedClient.UploadAsync("33", new NameValueCollection { { "Active", "false" } }, StreamOfLength(3));
			await shardedClient.UploadAsync("4", new NameValueCollection { { "Active", "false" } }, StreamOfLength(4));
			await shardedClient.UploadAsync("55555", new NameValueCollection { { "Active", "true" } }, StreamOfLength(5));

			var result = await shardedClient.SearchAsync("", new[] { "-Active" });
			var files = result.Files;

			Assert.Equal("true", files[0].Metadata["Active"]);
			Assert.Equal("true", files[1].Metadata["Active"]);
			Assert.Equal("false", files[2].Metadata["Active"]);
			Assert.Equal("false", files[3].Metadata["Active"]);
			Assert.Equal("false", files[4].Metadata["Active"]);
		}

        [Fact]
        public async Task CanTakeStats()
        {
            await shardedClient.UploadAsync("111", new NameValueCollection { { "Active", "true" } }, StreamOfLength(100));
            await shardedClient.UploadAsync("2", new NameValueCollection { { "Active", "false" } }, StreamOfLength(2));
            await shardedClient.UploadAsync("33", new NameValueCollection { { "Active", "false" } }, StreamOfLength(3));
            await shardedClient.UploadAsync("4", new NameValueCollection { { "Active", "false" } }, StreamOfLength(4));
            await shardedClient.UploadAsync("55555", new NameValueCollection { { "Active", "true" } }, StreamOfLength(5));

            var stats = await shardedClient.StatsAsync();

            Assert.NotNull(stats);
            Assert.Equal("shard1;shard2", stats.Name);
            Assert.NotNull(stats.Metrics);
            Assert.Equal(0, stats.ActiveSyncs.Count);
            Assert.Equal(0, stats.PendingSyncs.Count);
        }
	}
}
