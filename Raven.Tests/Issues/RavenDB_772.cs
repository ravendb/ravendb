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
				const int reduceOptimizationLimit = 4;

				documentStore.Configuration.NumberOfItemsToExecuteReduceInSingleStep = reduceOptimizationLimit;

				new Countries_ByAbbreviationAndName().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < reduceOptimizationLimit - 1; i++)
					{
						session.Store(new Country() { Name = "Poland", Abbreviation = "PL" });
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
					// add item with reduce key "IL" to switch into multi step reduce
					session.Store(new Country() { Name = "Israel", Abbreviation = "IL" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// should perform index with multi reduce for "IL" too
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "PL").Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "IL").Count);
				}
			}
		}

		[Fact]
		public void ShouldGetCorrectResultsIfNumberOfMappedItemsGoBelowAndNextAboveTheOptimizationLimit()
		{
			using (var documentStore = NewDocumentStore())
			{
				const int reduceOptimizationLimit = 4;

				documentStore.Configuration.NumberOfItemsToExecuteReduceInSingleStep = reduceOptimizationLimit;

				new Countries_ByAbbreviationAndName().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < reduceOptimizationLimit; i++)
					{
						session.Store(new Country() { Name = "Israel", Abbreviation = "IL" });
					}

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// here the reducing work will be done in multi step way
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "IL").Count);
				}

				using (var session = documentStore.OpenSession())
				{
					// delete one item to go below the reduceOptimizationLimit
					session.Delete(session.Load<Country>("countries/1"));

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// now the reduce will be done in single step (what's importatnt existing reduce results should be removed)
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(3, results.First(x => x.CountryAbbreviation == "IL").Count);
				}

				using (var session = documentStore.OpenSession())
				{
					// add item to go back to multi step reduce
					session.Store(new Country() { Name = "Israel", Abbreviation = "IL" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					// make sure that multi step reduce will return correct results
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "IL").Count);
				}
			}
		}

		[Fact]
		public void ShouldGetCorrectResultsIfMultiStepReduceWasUsedAndNextAllItemsWereRemoved()
		{
			using (var documentStore = NewDocumentStore())
			{
				const int reduceOptimizationLimit = 4;

				documentStore.Configuration.NumberOfItemsToExecuteReduceInSingleStep = reduceOptimizationLimit;

				new Countries_ByAbbreviationAndName().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < reduceOptimizationLimit; i++)
					{
						session.Store(new Country() { Name = "Israel", Abbreviation = "IL" });
					}

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(4, results.First(x => x.CountryAbbreviation == "IL").Count);
				}

				using (var session = documentStore.OpenSession())
				{
					// delete all items
					session.Delete(session.Load<Country>("countries/1"));
					session.Delete(session.Load<Country>("countries/2"));
					session.Delete(session.Load<Country>("countries/3"));
					session.Delete(session.Load<Country>("countries/4"));

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Countries_ByAbbreviationAndName.Result, Countries_ByAbbreviationAndName>()
										 .Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(0, results.Count);
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
			Map = countries => countries.Select(x => new { CountryAbbreviation = x.Abbreviation, Count = 1 });

			Reduce =
				results =>
				results.GroupBy(x => x.CountryAbbreviation)
					   .Select(g => new { CountryAbbreviation = g.Key, Count = g.Sum(c => c.Count) });
		}
	}
}
