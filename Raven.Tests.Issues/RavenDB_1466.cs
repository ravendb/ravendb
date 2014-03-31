// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1466.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1466 : RavenTestBase
	{
		public enum Region
		{
			North,
			South,
			East,
			West
		}

		public class Employee
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public Region Region { get; set; }
			public decimal Salary { get; set; }
		}

		public class EmployeeByRegionAndSalary : AbstractIndexCreationTask<Employee>
		{
			public EmployeeByRegionAndSalary()
			{
				Map = employees => from employee in employees select new {employee.Region, employee.Salary};
			}
		}

		[Fact]
		public void CanSearchByMultiFacetQueries_Remote()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage:"esent"))
			{
				DoTest(store);
			}
		}

		[Fact]
		public void CanSearchByMultiFacetQueries_Embedded()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				DoTest(store);
			}
		}

		private static void DoTest(IDocumentStore store)
		{
			new EmployeeByRegionAndSalary().Execute(store);

			using (var session = store.OpenSession())
			{
				session.Store(new Employee
				{
					Name = "A",
					Region = Region.North,
					Salary = 20000
				});

				session.Store(new Employee
				{
					Name = "B",
					Region = Region.North,
					Salary = 30000
				});

				session.Store(new Employee
				{
					Name = "C",
					Region = Region.South,
					Salary = 25000
				});

				session.Store(new Employee
				{
					Name = "D",
					Region = Region.East,
					Salary = 45000
				});

				session.Store(new Employee
				{
					Name = "E",
					Region = Region.East,
					Salary = 55000
				});

				session.Store(new Employee
				{
					Name = "F",
					Region = Region.West,
					Salary = 15000
				});

				session.Store(new Employee
				{
					Name = "G",
					Region = Region.West,
					Salary = 85000
				});

				session.SaveChanges();

				WaitForIndexing(store);

				var facets = new List<Facet>
				{
					new Facet<Employee>
					{
						Name = x => x.Salary,
						Ranges =
						{
							x => x.Salary < 20000,
							x => x.Salary >= 20000 && x.Salary < 40000,
							x => x.Salary >= 40000 && x.Salary < 60000,
							x => x.Salary >= 60000 && x.Salary < 80000,
							x => x.Salary > 80000
						}
					}
				};

				using (var s = store.OpenSession())
				{
					var facetSetupDoc = new FacetSetup {Id = "facets/EmployeeFacets", Facets = facets};
					s.Store(facetSetupDoc);
					s.SaveChanges();
				}

				// by using setup document

				var northSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
				                                   .Where(x => x.Region == Region.North).ToFacetQuery("facets/EmployeeFacets");
				var southSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
				                                   .Where(x => x.Region == Region.South).ToFacetQuery("facets/EmployeeFacets");
				var eastSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
				                                  .Where(x => x.Region == Region.East).ToFacetQuery("facets/EmployeeFacets");
				var westSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
				                                  .Where(x => x.Region == Region.West).ToFacetQuery("facets/EmployeeFacets");


				var multiFacetedSearchResults = session.Advanced.MultiFacetedSearch(northSalaryFacetQuery, southSalaryFacetQuery,
				                                                                    eastSalaryFacetQuery, westSalaryFacetQuery);

				Assert.Equal(4, multiFacetedSearchResults.Length);

				AssertResults(multiFacetedSearchResults);

				// by using list of facets

				northSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
											   .Where(x => x.Region == Region.North).ToFacetQuery(facets);
				southSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
											   .Where(x => x.Region == Region.South).ToFacetQuery(facets);
				eastSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
											  .Where(x => x.Region == Region.East).ToFacetQuery(facets);
				westSalaryFacetQuery = session.Query<Employee, EmployeeByRegionAndSalary>()
											  .Where(x => x.Region == Region.West).ToFacetQuery(facets);


				multiFacetedSearchResults = session.Advanced.MultiFacetedSearch(northSalaryFacetQuery, southSalaryFacetQuery,
																					eastSalaryFacetQuery, westSalaryFacetQuery);

				Assert.Equal(4, multiFacetedSearchResults.Length);

				AssertResults(multiFacetedSearchResults);
			}
		}

		private static void AssertResults(FacetResults[] multiFacetedSearchResults)
		{
			var northResults = multiFacetedSearchResults[0].Results["Salary_Range"].Values;
			Assert.Equal(1, northResults[0].Hits);
			Assert.Equal(1, northResults[1].Hits);
			Assert.Equal(0, northResults[2].Hits);
			Assert.Equal(0, northResults[3].Hits);
			Assert.Equal(0, northResults[4].Hits);

			var southResults = multiFacetedSearchResults[1].Results["Salary_Range"].Values;
			Assert.Equal(0, southResults[0].Hits);
			Assert.Equal(1, southResults[1].Hits);
			Assert.Equal(0, southResults[2].Hits);
			Assert.Equal(0, southResults[3].Hits);
			Assert.Equal(0, southResults[4].Hits);

			var eastResults = multiFacetedSearchResults[2].Results["Salary_Range"].Values;
			Assert.Equal(0, eastResults[0].Hits);
			Assert.Equal(0, eastResults[1].Hits);
			Assert.Equal(2, eastResults[2].Hits);
			Assert.Equal(0, eastResults[3].Hits);
			Assert.Equal(0, eastResults[4].Hits);

			var westResults = multiFacetedSearchResults[3].Results["Salary_Range"].Values;
			Assert.Equal(1, westResults[0].Hits);
			Assert.Equal(0, westResults[1].Hits);
			Assert.Equal(0, westResults[2].Hits);
			Assert.Equal(0, westResults[3].Hits);
			Assert.Equal(1, westResults[4].Hits);
		}
	}
}