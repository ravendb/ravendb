using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Encryption
{
	public class Indexes : Encryption
	{
		[Fact]
		public void SimpleIndexes()
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

			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
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
								Names = new[]{c.Name}
							}
						"
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


			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
		}
		

		[Fact]
		public void MapReduce()
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
								Names = new[]{c.Name}
							}
						",
					Reduce =
						@"
							from doc in results
							group doc by 1 into g
							select new
							{
								Names = g.SelectMany(x=>x.Names).Distinct()
							}
						"
				});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
                session.Advanced.DocumentQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<CompanyCount>("Name", "Count")
					.ToList();
			}

			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
		}
	}

	class CompanyCount
	{
		public string Name { get; set; }
		public int Count { get; set; }
	}
}
