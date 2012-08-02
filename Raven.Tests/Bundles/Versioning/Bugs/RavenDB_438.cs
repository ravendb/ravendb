using Raven.Client.Bundles.Versioning;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.Versioning.Bugs
{
	public class RavenDB_438 : VersioningTest
	{
		[Fact]
		public void Will_remove_items_from_index()
		{
			using (var s = documentStore.OpenSession())
			{
				s.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = false,
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 50
				}, "Raven/Versioning/DefaultConfiguration");
				s.SaveChanges();
			}

			for (int i = 0; i < 10; i++)
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company
					{
						Name = "Company " + i,
						Id = "companies/" + i
					}, "companies/" + i);
					session.SaveChanges();
				}
			}

			using (var session = documentStore.OpenSession())
			{
				var count = session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).Count();
				Assert.Equal(count, 10);
			}


			for (int i = 0; i < 10; i++)
			{
				documentStore.DatabaseCommands.Delete("companies/" + i, null);
			}

			using (var session = documentStore.OpenSession())
			{
				var count = session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).Count();
				Assert.Equal(count, 0);
			}
		}
	}
}