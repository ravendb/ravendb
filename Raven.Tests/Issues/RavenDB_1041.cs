// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1041.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1041 : ReplicationBase
	{
		class ReplicatedItem
		{
			public string Id { get; set; }
		}

		[Fact]
		public void CanWaitForReplication()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			((DocumentStore)store1).Replication.WaitAsync().Wait();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));
		}

		[Fact]
		public void CanWaitForReplicationOfParticularEtag()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			var putResult = store1.DatabaseCommands.Put("Replicated/1", null, new RavenJObject(), new RavenJObject());
			var putResult2 = store1.DatabaseCommands.Put("Replicated/2", null, new RavenJObject(), new RavenJObject());

			((DocumentStore)store1).Replication.WaitAsync(putResult.ETag).Wait();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));

			((DocumentStore)store1).Replication.WaitAsync(putResult2.ETag).Wait();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/2"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/2"));
		}

		[Fact]
		public async Task CanWaitForReplicationInAsyncManner()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}
			
			await ((DocumentStore)store1).Replication.WaitAsync();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));
		}

		[Fact]
		public void CanSpecifyTimeoutWhenWaitingForReplication()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url);

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			((DocumentStore)store1).Replication.WaitAsync(timeout: TimeSpan.FromSeconds(20)).Wait();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
		}

		[Fact]
		public async Task ShouldThrowTimeoutException()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, "http://localhost:1234"); // the last one is not running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			TimeoutException timeoutException = null;

			try
			{
				await ((DocumentStore) store1).Replication.WaitAsync(timeout: TimeSpan.FromSeconds(1), replicas: 2);
			}
			catch (TimeoutException ex)
			{
				timeoutException = ex;
			}

			Assert.NotNull(timeoutException);
			Assert.Contains("was replicated to 1 of 2 servers", timeoutException.Message);
		}

		[Fact]
		public void ShouldThrowIfCannotReachEnoughDestinationServers()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, "http://localhost:1234", "http://localhost:1235"); // non of them is running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			var exception = Assert.Throws<AggregateException>(() => ((DocumentStore)store1).Replication.WaitAsync(replicas: 3).Wait());

			Assert.Equal(2, ((AggregateException)exception.InnerExceptions[0]).InnerExceptions.Count);
		}

		[Fact]
		public async Task CanWaitForReplicationForOneServerEvenIfTheSecondOneIsDown()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, "http://localhost:1234"); // the last one is not running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			await ((DocumentStore) store1).Replication.WaitAsync(replicas: 1);

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
		}
	}
}
