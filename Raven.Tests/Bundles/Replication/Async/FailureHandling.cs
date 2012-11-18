//-----------------------------------------------------------------------
// <copyright file="FailureHandling.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Async
{
	public class FailureHandling : ReplicationBase
	{
		[Fact]
		public void When_replicating_from_two_different_source_different_documents_at_the_same_time()
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

			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store3.OpenAsyncSession())
				{
					if(session.LoadAsync<Company>("companies/1").Result != null)
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
