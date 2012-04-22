//-----------------------------------------------------------------------
// <copyright file="ConflictWhenReplicating.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading;
using Raven.Bundles.Tests.Versioning;
using Raven.Client.Exceptions;
using Xunit;

namespace Raven.Bundles.Tests.Replication.Async
{
	public class ConflictWhenReplicating : ReplicationBase
	{
		[Fact]
		public void When_replicating_and_a_document_is_already_there_will_result_in_conflict()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			using(var session = store1.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			using (var session = store2.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			TellFirstInstanceToReplicateToSecondInstance();

			var aggregateException = Assert.Throws<AggregateException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store2.OpenAsyncSession())
					{
						session.LoadAsync<Company>("companies/1").Wait();
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}

		[Fact]
		public void When_replicating_from_two_different_source_different_documents()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();
			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			using (var session = store2.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			TellInstanceToReplicateToAnotherInstance(0,2);

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (session.LoadAsync<Company>("companies/1").Result != null)
						break;
					Thread.Sleep(100);
				}
			}

			TellInstanceToReplicateToAnotherInstance(1, 2);

			var aggregateException = Assert.Throws<AggregateException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenAsyncSession())
					{
						session.LoadAsync<Company>("companies/1").Wait();
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}

		[Fact]
		public void Can_confclit_on_deletes_as_well()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();
			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			using (var session = store2.OpenAsyncSession())
			{
				session.Store(new Company());
				session.SaveChangesAsync().Wait();
			}

			TellInstanceToReplicateToAnotherInstance(0, 2);

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (session.LoadAsync<Company>("companies/1").Result != null)
						break;
					Thread.Sleep(100);
				}
			}

			using (var session = store2.OpenAsyncSession())
			{
				session.Delete(session.LoadAsync<Company>("companies/1").Result);
				session.SaveChangesAsync().Wait();
			}

			for (int i = 0; i < RetriesCount; i++) // wait for it to NOT show up in the 3rd server
			{
				using (var session = store3.OpenAsyncSession())
				{
					if (session.LoadAsync<Company>("companies/1").Result == null)
						break;
					Thread.Sleep(100);
				}
			}


			TellInstanceToReplicateToAnotherInstance(1, 2);

			var aggregateException = Assert.Throws<AggregateException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenAsyncSession())
					{
						session.LoadAsync<Company>("companies/1").Wait();
						Thread.Sleep(100);
					}
				}
			});
			var conflictException = Assert.IsType<ConflictException>(aggregateException.Flatten().InnerException);

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}
	}
}
