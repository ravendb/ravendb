using System.Threading;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class ReplicatingDanish : ReplicationBase
	{
		[Fact]
		public void Storing_document_with_non_english_characters_should_work()
		{
			using (var store = CreateStore(useFiddler:true))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Company { Name = "æÆ,Øø,åÅ" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var company = session.Load<Company>("companies/1");

					Assert.NotNull(company);
					Assert.Equal("æÆ,Øø,åÅ", company.Name);
				}
			}
		}

		[Fact]
		public void Can_replicate_between_two_instances()
		{
			using(var store1 = CreateStore(useFiddler:true))
			using(var store2 = CreateStore(useFiddler:true))
			{
				TellFirstInstanceToReplicateToSecondInstance();

				using (var session = store1.OpenSession())
				{
					session.Store(new Company { Name = "æÆ,Øø,åÅ" });
					session.SaveChanges();
				}


				Company company = null;
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store2.OpenSession())
					{
						company = session.Load<Company>("companies/1");
						if (company != null)
							break;
						Thread.Sleep(100);
					}
				}
				Assert.NotNull(company);
				Assert.Equal("æÆ,Øø,åÅ", company.Name);
			}
		}
	}
}