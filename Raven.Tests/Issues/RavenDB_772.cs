using System.Linq;

namespace Raven.Tests.Issues
{
	using Client.Indexes;
	using Xunit;

	public class RavenDB_772 : RavenTest
	{
		[Fact]
		public void ShouldGetCorrectResultsIfReduceOptimizationWasApplied()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.Configuration.NumberOfItemsToExecuteReduceInSingleStep = 4;

				new Countries_ByAbbreviationAndName().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < 3; i++)
					{
						session.Store(new Country(){Name = "Poland", Abbreviation = "PL"});
						session.Store(new Country() { Name = "Israel", Abbreviation = "IL" });
					}
					session.Store(new Country() { Name = "Poland", Abbreviation = "PL" });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// should perform index with single reduce step for "IL" and multi reduce for "PL"
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
					                     .Customize(x => x.WaitForNonStaleResults()).ToList();
					
					Assert.Equal(2, results.Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "PL").Count);
					Assert.Equal(3, results.First(x => x.CountryAbbreviation == "IL").Count);
				}

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Country() { Name = "Israel", Abbreviation = "PL" });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// should perform index with multi reduce for "IL" too
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(5, results.First(x => x.CountryAbbreviation == "PL").Count);
					Assert.Equal(3, results.First(x => x.CountryAbbreviation == "IL").Count);
				}
			}
		}
	}

	class Country
	{
		public string Abbreviation { get; set; }

		public string Name { get; set; }
	}

	class Countries_ByAbbreviationAndName : AbstractIndexCreationTask<Country, Countries_ByAbbreviationAndName.Result>
	{
		public class Result
		{
			public string CountryAbbreviation { get; set; }

			public int Count { get; set; }
		}

		public Countries_ByAbbreviationAndName()
		{
			Map = countries => countries.Select(x => new {CountryAbbreviation = x.Abbreviation, Count = 1});

			Reduce =
				results =>
				results.GroupBy(x => x.CountryAbbreviation)
				       .Select(g => new {CountryAbbreviation = g.Key, Count = g.Sum(c => c.Count)});
		}
	}
}
