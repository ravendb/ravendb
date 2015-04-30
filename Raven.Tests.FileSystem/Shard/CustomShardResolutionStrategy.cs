// -----------------------------------------------------------------------
//  <copyright file="CustomShardResolutionStrategy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Shard;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Shard
{
	public class CustomShardResolutionStrategy : RavenFilesTestBase
	{
		public class RegionMetadataBasedResolutionStrategy : IShardResolutionStrategy
		{
			private int counter;
			private readonly IList<string> shardIds;
			private readonly ShardStrategy.ModifyFileNameFunc modifyFileName;
			private readonly FilesConvention conventions;

			public RegionMetadataBasedResolutionStrategy(IList<string> shardIds, ShardStrategy.ModifyFileNameFunc modifyFileName, FilesConvention conventions)
			{
				this.shardIds = shardIds;
				this.modifyFileName = modifyFileName;
				this.conventions = conventions;
			}

			public ShardResolutionResult GetShardIdForUpload(string filename, RavenJObject metadata)
			{
				var shardId = GenerateShardIdFor(filename, metadata);

				return new ShardResolutionResult
				{
					ShardId = shardId,
					NewFileName = modifyFileName(conventions, shardId, filename)
				};
			}

			public string GetShardIdFromFileName(string filename)
			{
				if (filename.StartsWith("/"))
					filename = filename.TrimStart(new[] { '/' });
				var start = filename.IndexOf(conventions.IdentityPartsSeparator, StringComparison.OrdinalIgnoreCase);
				if (start == -1)
					throw new InvalidDataException("file name does not have the required file name");

				var maybeShardId = filename.Substring(0, start);

				if (shardIds.Any(x => string.Equals(maybeShardId, x, StringComparison.OrdinalIgnoreCase)))
					return maybeShardId;

				throw new InvalidDataException("could not find a shard with the id: " + maybeShardId);
			}

			public string GenerateShardIdFor(string filename, RavenJObject metadata)
			{
				// choose shard based on the region
				var region = metadata.Value<string>("Region");

				string shardId = null;

				if (string.IsNullOrEmpty(region) == false)
					shardId = shardIds.FirstOrDefault(x => x.Equals(region, StringComparison.OrdinalIgnoreCase));

				return shardId ?? shardIds[Interlocked.Increment(ref counter) % shardIds.Count];
			}

			public IList<string> PotentialShardsFor(ShardRequestData requestData)
			{
				// for future use
				throw new NotImplementedException();
			}
		}

		[Fact]
		public async Task ShouldWork()
		{
			var client1 = NewAsyncClient(0, fileSystemName: "shard1");
			var client2 = NewAsyncClient(1, fileSystemName: "shard2");

			var shards = new Dictionary<string, IAsyncFilesCommands>
		    {
			    {"Europe", client1},
			    {"Asia", client2},
		    };

			var strategy = new ShardStrategy(shards);

			strategy.ShardResolutionStrategy = new RegionMetadataBasedResolutionStrategy(shards.Keys.ToList(), strategy.ModifyFileName, strategy.Conventions);

			var client = new AsyncShardedFilesServerClient(strategy);

			var fileName = await client.UploadAsync("test1", new RavenJObject()
		    {
			    {
				    "Region", "Europe"
			    }
		    }, new MemoryStream());

			Assert.Equal("/Europe/test1", fileName);

			fileName = await client.UploadAsync("test2", new RavenJObject()
		    {
			    {
				    "region", "asia"
			    }
		    }, new MemoryStream());

			Assert.Equal("/Asia/test2", fileName);
		}
	}
}