// //-----------------------------------------------------------------------
// // <copyright file="David.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Client;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class David : ReplicationBase
	{
		[Fact]
		public void Can_replicate_between_two_instances_create_delete_create()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			// create
			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "HR" }, Guid.Empty, "companies/1");
				session.SaveChanges();
			}

			// wait for replication
			WaitForReplication(store2, documentMissing: false);

			// delete
			using (var session = store1.OpenSession())
			{
				session.Delete(session.Load<Company>(1));
				session.SaveChanges();
			}

			// wait for replication of delete
			WaitForReplication(store2, documentMissing: true);

			// create
			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" }, Guid.Empty, "companies/1");
				session.SaveChanges();
			}

			// wait for replication
			WaitForReplication(store2, documentMissing: false);

			using (var session = store1.OpenSession())
			{
				Assert.Equal("Hibernating Rhinos", session.Load<Company>(1).Name);
			}
		}


		[Fact]
		public void Can_replicate_between_two_instances_create_delete_create_quickly()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			for (int i = 0; i < 10; i++)
			{

				// create
				using (var session = store1.OpenSession())
				{
					session.Store(new Company { Name = "HR" }, Guid.Empty, "companies/1");
					session.SaveChanges();
				}

				// delete
				using (var session = store1.OpenSession())
				{
					session.Delete(session.Load<Company>(1));
					session.SaveChanges();
				}
			}

			// wait for replication of delete
			WaitForReplication(store2, documentMissing: true);

			// create
			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" }, Guid.Empty, "companies/1");
				session.SaveChanges();
			}

			// wait for replication
			WaitForReplication(store2, documentMissing: false);

			using (var session = store1.OpenSession())
			{
				Assert.Equal("Hibernating Rhinos", session.Load<Company>(1).Name);
			}
		}

		private void WaitForReplication(IDocumentStore store2, bool documentMissing)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenSession())
				{
					var company = session.Load<Company>("companies/1");
					if ((company == null) == documentMissing)
						break;
					Thread.Sleep(100);
				}
			}
		}
	}
}