//-----------------------------------------------------------------------
// <copyright file="FailureHandling.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class FailureHandling : ReplicationBase
	{
		[Fact]
		public void When_replicating_from_two_different_source_different_documents_at_the_same_time()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();
			using (var session = store1.OpenSession())
			{
				session.Store(new Company());
				session.SaveChanges();
			}

			using (var session = store2.OpenSession())
			{
				session.Store(new Company());
				session.SaveChanges();
			}

			TellInstanceToReplicateToAnotherInstance(0, 2);

			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store3.OpenSession())
				{
					if(session.Load<Company>("companies/1") != null)
						break;
					Thread.Sleep(100);
				}
			}

			TellInstanceToReplicateToAnotherInstance(1, 2);

			var conflictException = Assert.Throws<ConflictException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenSession())
					{
						session.Load<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}
	}
}
