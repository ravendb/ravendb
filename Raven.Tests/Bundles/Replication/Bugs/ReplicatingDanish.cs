using System.Threading;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class ReplicatingDanish : ReplicationBase
	{

		[Fact]
		public void Can_replicate_between_two_instances()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Ê∆,ÿ¯,Â≈" });
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
			Assert.Equal("Ê∆,ÿ¯,Â≈", company.Name);
		}
	}
}