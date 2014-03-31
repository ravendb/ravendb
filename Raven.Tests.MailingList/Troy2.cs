using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Troy2 : RavenTest
	{
		[Fact]
		public void UsingDefaultFieldWithSelectFieldsFails()
		{
			using (var store = NewDocumentStore())
			{
				new TesterSearch().Execute(store);
				using (var session = store.OpenSession())
				{

					var testClasses = new List<Tester>
					{
						new Tester
						{
							FirstName = "FirstName 1",
							LastName = "LastName 1",
							Email = "email1@test.com",
							Password = "test1"
						},
						new Tester
						{
							FirstName = "FirstName 2",
							LastName = "LastName 2",
							Email = "email2@test.com",
							Password = "test2"
						}
					};
					testClasses.ForEach(session.Store);
					session.SaveChanges();

					RavenQueryStatistics stats;
                    var query = session.Advanced.DocumentQuery<Tester, TesterSearch>()
									   .WaitForNonStaleResults()
									   .Statistics(out stats)
									   .UsingDefaultField("Query")
									   .OpenSubclause()
									   .Where("FirstName*")
									   .CloseSubclause()
									   .AndAlso()
									   .WhereEquals(x => x.Email, "email1@test.com")
									   .OrderBy("+LastName")
									   .Skip(0)
									   .Take(10)
									   .ToList();
					Assert.Equal(1, stats.TotalResults);

                    var selectFieldsQuery = session.Advanced.DocumentQuery<Tester, TesterSearch>()
								   .WaitForNonStaleResults()
								   .Statistics(out stats)
								   .UsingDefaultField("Query")
								   .OpenSubclause()
								   .Where("FirstName*")
								   .CloseSubclause()
								   .AndAlso()
								   .WhereEquals(x => x.Email, "email1@test.com")
								   .OrderBy("+LastName")
								   .SelectFields<PasswordOnly>()
								   .Skip(0)
								   .Take(10)
								   .ToList();
					Assert.Equal(1, stats.TotalResults);

				}
			}
		}

		public class TesterSearch : AbstractIndexCreationTask<Tester, TesterSearch.SearchResult>
		{

			public override string IndexName
			{
				get
				{
					return "Tester/Search";
				}
			}

			public class SearchResult
			{
				public string Query { get; set; }
				public string FirstName { get; set; }
				public string LastName { get; set; }
				public string Email { get; set; }
				public string Password { get; set; }
			}

			public TesterSearch()
			{
				Map = testClasses =>
						from testClass in testClasses
						select new
						{
							Query = new object[]
							{
								testClass.FirstName,
								testClass.LastName,
								testClass.Email
							},
							testClass.FirstName,
							testClass.LastName,
							testClass.Email,
							testClass.Password
						};

				Index(x => x.Query, FieldIndexing.Analyzed);
				Index(x => x.FirstName, FieldIndexing.Default);
				Index(x => x.LastName, FieldIndexing.Default);
				Index(x => x.Email, FieldIndexing.Default);
				Index(x => x.Password, FieldIndexing.Default);
			}
		}

		public class PasswordOnly
		{
			public string Password { get; set; }
		}

		public class Tester
		{
			public string Id { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string Email { get; set; }
			public string Password { get; set; }
		}
	}
}