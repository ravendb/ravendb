//-----------------------------------------------------------------------
// <copyright file="ConflictWhenReplicating.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net;
using System.Threading;
using Raven.Bundles.Tests.Versioning;
using Raven.Client.Exceptions;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
	public class ConflictWhenReplicating : ReplicationBase
	{
		[Fact]
		public void When_replicating_and_a_document_is_already_there_will_result_in_conflict()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			using(var session = store1.OpenSession())
			{
				session.Store(new Company());
				session.SaveChanges();
			}

			using (var session = store2.OpenSession())
			{
				session.Store(new Company());
				session.SaveChanges();
			}

			TellFirstInstanceToReplicateToSecondInstance();

			var conflictException = Assert.Throws<ConflictException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store2.OpenSession())
					{
						session.Load<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});

			Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
		}

		[Fact]
		public void Can_resolve_conflict_by_deleting_conflicted_doc()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
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

			TellFirstInstanceToReplicateToSecondInstance();

			var conflictException = Assert.Throws<ConflictException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store2.OpenSession())
					{
						session.Load<Company>("companies/1");
						Thread.Sleep(100);
					}
				}
			});

			store2.DatabaseCommands.Delete("companies/1", null);

			foreach (var conflictedVersionId in conflictException.ConflictedVersionIds)
			{
				Assert.Null(store2.DatabaseCommands.Get(conflictedVersionId));
			}
		}

		[Fact]
		public void When_replicating_from_two_different_source_different_documents()
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

			TellInstanceToReplicateToAnotherInstance(0,2);

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenSession())
				{
					if (session.Load<Company>("companies/1") != null)
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

		[Fact]
		public void Can_confclit_on_deletes_as_well()
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

			for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
			{
				using (var session = store3.OpenSession())
				{
					if (session.Load<Company>("companies/1") != null)
						break;
					Thread.Sleep(100);
				}
			}

			using (var session = store1.OpenSession())
			{
				session.Delete(session.Load<Company>("companies/1"));
				session.SaveChanges();
			}

			for (int i = 0; i < RetriesCount; i++) // wait for it to NOT show up in the 3rd server
			{
				using (var session = store3.OpenSession())
				{
					if (session.Load<Company>("companies/1") == null)
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
