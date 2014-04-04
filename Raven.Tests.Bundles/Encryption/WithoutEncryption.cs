using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Encryption
{
	public class WithoutEncryption : Encryption
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "none";
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
                session.Advanced.DocumentQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}

			RecycleServer();

			using (var session = documentStore.OpenSession())
			{
                session.Advanced.DocumentQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}
		}
	}
}