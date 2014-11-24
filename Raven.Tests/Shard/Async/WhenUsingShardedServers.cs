//-----------------------------------------------------------------------
// <copyright file="WhenUsingShardedServers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Database.Server;
using Raven.Server;
using Raven.Tests.Common;
using Raven.Tests.Document;
using Rhino.Mocks;
using Xunit;

namespace Raven.Tests.Shard.Async
{
	public class WhenUsingShardedServers : RavenTest
	{
		readonly RavenDbServer server1;
		readonly RavenDbServer server2;
		readonly Company company1;
		readonly Company company2;
		readonly IDictionary<string, IDocumentStore> shards;
		readonly IShardResolutionStrategy shardResolution;
		readonly ShardStrategy shardStrategy;

		public WhenUsingShardedServers()
		{
			const string server = "localhost";

			const int port1 = 8079;
			const int port2 = 8081;

			company1 = new Company { Name = "Company1" };
			company2 = new Company { Name = "Company2" };

			server1 = GetNewServer(port1);
			server2 = GetNewServer(port2);

			shards = new List<IDocumentStore> { 
				new DocumentStore { Identifier="Shard1", Url = "http://" + server +":"+port1}, 
				new DocumentStore { Identifier="Shard2", Url = "http://" + server +":"+port2} 
			}.ToDictionary(x => x.Identifier, x => x);

			shardResolution = MockRepository.GenerateStub<IShardResolutionStrategy>();
			shardResolution.Stub(x => x.GenerateShardIdFor(Arg.Is(company1), Arg<ITransactionalDocumentSession>.Is.Anything)).Return("Shard1");
			shardResolution.Stub(x => x.GenerateShardIdFor(Arg.Is(company2), Arg<ITransactionalDocumentSession>.Is.Anything)).Return("Shard2");

			shardResolution.Stub(x => x.MetadataShardIdFor(company1)).Return("Shard1");
			shardResolution.Stub(x => x.MetadataShardIdFor(company2)).Return("Shard1");

			shardStrategy = new ShardStrategy(shards) { ShardResolutionStrategy = shardResolution };
		}

		[Fact]
		public async Task CanOverrideTheShardIdGeneration()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				foreach (var shard in shards)
				{
					shard.Value.Conventions.DocumentKeyGenerator = (dbName, cmds, c) => ((Company)c).Name;
				}

				using (var session = documentStore.OpenAsyncSession())
				{
					await session.StoreAsync(company1);
					await session.StoreAsync(company2);

					await session.SaveChangesAsync();

					Assert.Equal("Shard1/companies/1", company1.Id);
					Assert.Equal("Shard2/companies/2", company2.Id);
				}
			}
		}

		[Fact]
		public async Task CanQueryUsingInt()
		{
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				using (var session = documentStore.OpenAsyncSession())
				{
					await session.LoadAsync<Company>(1);
				}
			}
		}

		[Fact]
		public async Task CanInsertIntoTwoShardedServers()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				using (var session = documentStore.OpenAsyncSession())
				{
					await session.StoreAsync(company1);
					await session.StoreAsync(company2);
					await session.SaveChangesAsync();
				}
			}
		}

		[Fact]
		public async Task CanGetSingleEntityFromCorrectShardedServer()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenAsyncSession())
			{
				//store item that goes in 2nd shard
				await session.StoreAsync(company2);
				await session.SaveChangesAsync();

				//get it, should automagically retrieve from 2nd shard
				shardResolution.Stub(x => x.PotentialShardsFor(null)).IgnoreArguments().Return(new[] { "Shard2" });
				var loadedCompany = await session.LoadAsync<Company>(company2.Id);

				Assert.NotNull(loadedCompany);
				Assert.Equal(company2.Name, loadedCompany.Name);
			}
		}

		[Fact]
		public async Task CanGetSingleEntityFromCorrectShardedServerWhenLocationIsUnknown()
		{
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenAsyncSession())
			{
				//store item that goes in 2nd shard
				await session.StoreAsync(company2);
				await session.SaveChangesAsync();

				//get it, should try all shards and find it
				shardResolution.Stub(x => x.PotentialShardsFor(null)).IgnoreArguments().Return(null);
				var loadedCompany = await session.LoadAsync<Company>(company2.Id);

				Assert.NotNull(loadedCompany);
				Assert.Equal(company2.Name, loadedCompany.Name);
			}
		}

		[Fact]
		public async Task CanGetAllShardedEntities()
		{
			//get them in simple single threaded sequence for this test
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenAsyncSession())
			{
				//store 2 items in 2 shards
				await session.StoreAsync(company1);
				await session.StoreAsync(company2);

				await session.SaveChangesAsync();


				//get all, should automagically retrieve from each shard
				var allCompanies = (await session.Advanced.AsyncDocumentQuery<Company>()
				                                 .WaitForNonStaleResults()
				                                 .ToListAsync());

				Assert.NotNull(allCompanies);
				Assert.Equal(company1.Name, allCompanies[0].Name);
				Assert.Equal(company2.Name, allCompanies[1].Name);
			}
		}
	}
}
