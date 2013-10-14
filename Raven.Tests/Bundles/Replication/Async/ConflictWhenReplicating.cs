//-----------------------------------------------------------------------
// <copyright file="ConflictWhenReplicating.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Async
{
	public class ConflictWhenReplicating : ReplicationBase
	{
		[Fact]
		public async Task When_replicating_and_a_document_is_already_there_will_result_in_conflict()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			using(var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			using (var session = store2.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			TellFirstInstanceToReplicateToSecondInstance();

			var aggregateException = await AssertAsync.Throws<AggregateException>(async () =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store2.OpenAsyncSession())
					{
						await session.LoadAsync<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}

		[Fact]
		public async Task When_replicating_from_two_different_source_different_documents()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();
			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			using (var session = store2.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			TellInstanceToReplicateToAnotherInstance(0,2);

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (await session.LoadAsync<Company>("companies/1") != null)
						break;
					Thread.Sleep(100);
				}
			}

			TellInstanceToReplicateToAnotherInstance(1, 2);

			var aggregateException = await AssertAsync.Throws<AggregateException>(async () =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenAsyncSession())
					{
						await session.LoadAsync<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}

		[Fact]
		public async Task Can_conflict_on_deletes_as_well()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();
			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			using (var session = store2.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			TellInstanceToReplicateToAnotherInstance(0, 2);

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (await session.LoadAsync<Company>("companies/1") != null)
						break;
					Thread.Sleep(100);
				}
			}

			using (var session = store2.OpenAsyncSession())
			{
				session.Delete(await session.LoadAsync<Company>("companies/1"));
				await session.SaveChangesAsync();
			}

			for (int i = 0; i < RetriesCount; i++) // wait for it to NOT show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (await session.LoadAsync<Company>("companies/1") == null)
						break;
					Thread.Sleep(100);
				}
			}


			TellInstanceToReplicateToAnotherInstance(1, 2);

			var aggregateException = await AssertAsync.Throws<AggregateException>(async () =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenAsyncSession())
					{
						await session.LoadAsync<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}
	}
}