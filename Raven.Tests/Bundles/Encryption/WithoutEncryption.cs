using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Encryption
{
	public class WithoutEncryption : Encryption
	{
		protected override void ConfigureServer(Raven.Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.Settings["Raven/ActiveBundles"] = "none";
		}

		[Fact]
		public void Restart()
		{
			const string FirstCompany = "FirstCompany";
			const string SecondCompany = "SecondCompany";
			const string IndexName = "TestIndex";

			documentStore.DatabaseCommands.PutIndex(IndexName,
				new IndexDefinition
				{
					Map =
						@"
							from c in docs.Companies
							select new 
								{
									c.Name
								}
						",
					Stores =
					{
						{ "Name", FieldStorage.Yes }
					}
				});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}

			RecycleServer();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}
		}
		

	
	}
}