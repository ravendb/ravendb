//-----------------------------------------------------------------------
// <copyright file="SimpleReplication.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Async
{
	public class SimpleReplication : ReplicationBase
	{
		[Fact]
		public async Task Can_replicate_between_two_instances()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			Company company = WaitForDocument<Company>(store2, "companies/1");
			Assert.Equal("Hibernating Rhinos", company.Name);
		}

		[Fact]
		public async Task Can_replicate_large_number_of_documents_between_two_instances()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenAsyncSession())
			{
				for (int i = 0; i < 150; i++)
				{
					await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				}
				await session.SaveChangesAsync();
			}


			using (var session = store2.OpenAsyncSession())
			{
				session.Advanced.MaxNumberOfRequestsPerSession = RetriesCount * 2;

				bool foundAll = false;
				for (int i = 0; i < RetriesCount; i++)
				{
					var countFound = 0;
					for (int j = 0; j < 150; j++)
					{
						var company = await session.LoadAsync<Company>("companies/" + (i + 1));
						if (company == null)
							break;
						countFound++;
					}
					foundAll = countFound == 150;
					if (foundAll)
						break;
					Thread.Sleep(100);
				}
				Assert.True(foundAll);
			}
		}

		[Fact]
		public async Task Will_not_replicate_replicated_documents()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			TellSecondInstanceToReplicateToFirstInstance();
			Company company = null;

			string etag;
			string id;
			using (var session = store1.OpenAsyncSession())
			{
				 company = new Company { Name = "Hibernating Rhinos" };
				await session.StoreAsync(company);
				await session.SaveChangesAsync();
				id = company.Id;
				session.Advanced.Clear();
				company = await session.LoadAsync<Company>(id);
				etag = session.Advanced.GetMetadataFor(company).Value<string>("@etag");
			}



			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenAsyncSession()) // waiting for document to show up.
				{
					company = await session.LoadAsync<Company>(id);
					if (company != null)
						break;
					Thread.Sleep(100);

				}
			}
			Assert.NotNull(company);
			Assert.Equal("Hibernating Rhinos", company.Name);

			// assert that the etag haven't changed (we haven't replicated)
			for (int i = 0; i < 15; i++)
			{
				using (var session = store1.OpenAsyncSession())
				{
					company = await session.LoadAsync<Company>(id);
					Assert.Equal(etag, session.Advanced.GetMetadataFor(company).Value<string>("@etag"));
				}
				Thread.Sleep(100);
			}
		}

		[Fact]
		public async Task Can_replicate_delete_between_two_instances()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			Company company = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenAsyncSession())
				{
					company = await session.LoadAsync<Company>("companies/1");
					if (company != null)
						break;
					Thread.Sleep(100);
				}
			}
			Assert.NotNull(company);
			Assert.Equal("Hibernating Rhinos", company.Name);

			using (var session = store1.OpenAsyncSession())
			{
				session.Delete(await session.LoadAsync<Company>("companies/1"));
				await session.SaveChangesAsync();
			}


			Company deletedCompany = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenAsyncSession())
					deletedCompany = await session.LoadAsync<Company>("companies/1");
				if (deletedCompany == null)
					break;
				Thread.Sleep(100);
			}
			Assert.Null(deletedCompany);
		}
	}
}