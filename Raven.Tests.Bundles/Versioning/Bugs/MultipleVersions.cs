using Raven.Client.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Versioning.Bugs
{
	public class MultipleVersions : VersioningTest
	{

		[Fact]
		public void Will_automatically_set_metadata()
		{
			using(var s = documentStore.OpenSession())
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
						Id = "companies/1"
					}, "companies/1");
					session.SaveChanges();
				}
			}

			using (var session = documentStore.OpenSession())
			{
				var company = session.Load<Company>("companies/1");
				var companies = session.Advanced.GetRevisionsFor<Company>(company.Id, 0, 15);
				Assert.Equal(10, companies.Length);
			}
		}
	}
}