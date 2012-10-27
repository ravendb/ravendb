//-----------------------------------------------------------------------
// <copyright file="WhenUsingShardedServers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Raven.Tests.Document;
using Rhino.Mocks;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard;
using System.Linq;

namespace Raven.Tests.Shard
{
	using Raven.Abstractions.Data;

	public class WhenUsingShardedServers : RemoteClientTest, IDisposable
	{
		readonly string path1;
		readonly string path2;
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

			path1 = GetPath("TestShardedDb1");
			path2 = GetPath("TestShardedDb2");

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port1);
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port2);

			company1 = new Company { Name = "Company1" };
			company2 = new Company { Name = "Company2" };

			server1 = GetNewServer(port1, path1);
			server2 = GetNewServer(port2, path2);

			shards = new List<IDocumentStore> { 
				new DocumentStore { Identifier="Shard1", Url = "http://" + server +":"+port1}, 
				new DocumentStore { Identifier="Shard2", Url = "http://" + server +":"+port2} 
			}.ToDictionary(x => x.Identifier, x => x);

			shardResolution = MockRepository.GenerateStub<IShardResolutionStrategy>();
			shardResolution.Stub(x => x.GenerateShardIdFor(Arg.Is(company1), Arg<SessionMetadata>.Is.Anything)).Return("Shard1");
			shardResolution.Stub(x => x.GenerateShardIdFor(Arg.Is(company2), Arg<SessionMetadata>.Is.Anything)).Return("Shard2");

			shardResolution.Stub(x => x.MetadataShardIdFor(company1)).Return("Shard1");
			shardResolution.Stub(x => x.MetadataShardIdFor(company2)).Return("Shard1");

			shardStrategy = new ShardStrategy(shards) { ShardResolutionStrategy = shardResolution };
		}

		[Fact]
		public void CanOverrideTheShardIdGeneration()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				foreach (var shard in shards)
				{
					shard.Value.Conventions.DocumentKeyGenerator = (cmds, c) => ((Company)c).Name;
				}

				using (var session = documentStore.OpenSession())
				{
					session.Store(company1);
					session.Store(company2);

					Assert.Equal("Shard1/companies/1", company1.Id);
					Assert.Equal("Shard2/companies/2", company2.Id);
				}
			}
		}

		[Fact]
		public void CanQueryUsingInt()
		{
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				using (var session = documentStore.OpenSession())
				{
					session.Load<Company>(1);
				}
			}
		}

		[Fact]
		public void CanInsertIntoTwoShardedServers()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy))
			{
				documentStore.Initialize();

				using (var session = documentStore.OpenSession())
				{
					session.Store(company1);
					session.Store(company2);
					session.SaveChanges();
				}
			}
		}

		[Fact]
		public void CanGetSingleEntityFromCorrectShardedServer()
		{
			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenSession())
			{
				//store item that goes in 2nd shard
				session.Store(company2);
				session.SaveChanges();

				//get it, should automagically retrieve from 2nd shard
				shardResolution.Stub(x => x.PotentialShardsFor(null)).IgnoreArguments().Return(new[] { "Shard2" });
				var loadedCompany = session.Load<Company>(company2.Id);

				Assert.NotNull(loadedCompany);
				Assert.Equal(company2.Name, loadedCompany.Name);
			}
		}

		[Fact]
		public void CanGetSingleEntityFromCorrectShardedServerWhenLocationIsUnknown()
		{
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenSession())
			{
				//store item that goes in 2nd shard
				session.Store(company2);

				session.SaveChanges();

				//get it, should try all shards and find it
				shardResolution.Stub(x => x.PotentialShardsFor(null)).IgnoreArguments().Return(null);
				var loadedCompany = session.Load<Company>(company2.Id);

				Assert.NotNull(loadedCompany);
				Assert.Equal(company2.Name, loadedCompany.Name);
			}
		}

		[Fact]
		public void CanGetAllShardedEntities()
		{
			//get them in simple single threaded sequence for this test
			shardStrategy.ShardAccessStrategy = new SequentialShardAccessStrategy();

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			using (var session = documentStore.OpenSession())
			{
				//store 2 items in 2 shards
				session.Store(company1);
				session.Store(company2);

				session.SaveChanges();


				//get all, should automagically retrieve from each shard
				var allCompanies = session.Advanced.LuceneQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.NotNull(allCompanies);
				Assert.Equal(company1.Name, allCompanies[0].Name);
				Assert.Equal(company2.Name, allCompanies[1].Name);
			}
		}

		public override void Dispose()
		{
			server1.Dispose();
			server2.Dispose();

			Thread.Sleep(100);

			foreach (var path in new[] { path1, path2 })
			{
				try
				{
					IOExtensions.DeleteDirectory(path);
				}
				catch (Exception) { }
			}
			base.Dispose();
		}
	}
}