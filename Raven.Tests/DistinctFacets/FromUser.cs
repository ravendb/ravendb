// -----------------------------------------------------------------------
//  <copyright file="FromUser.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.DistinctFacets
{
	public class FromUser : RavenTest
	{
		[Fact]
		public void ShouldFacetsWork()
		{
			using (var documentStore = NewDocumentStore())
			{
				CreateSampleData(documentStore);
				WaitForIndexing(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var result = session.Advanced.DocumentQuery<SampleData, SampleData_Index>()
						.Distinct()
						.SelectFields<SampleData_Index.Result>("Name")
						.ToFacets(new[]
						{
							new Facet
							{
								Name = "Tag"
							},
							new Facet
							{
								Name = "TotalCount"
							}, 
						});
                    WaitForUserToContinueTheTest(documentStore);
					Assert.Equal(3, result.Results["Tag"].Values.Count);

					Assert.Equal(5, result.Results["TotalCount"].Values[0].Hits);

					Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "0").Hits);
					Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "1").Hits);
					Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "2").Hits);
				}
			}
		}
		private  void CreateSampleData(IDocumentStore documentStore)
		{
			var names = new List<string>() { "Raven", "MSSQL", "NoSQL", "MYSQL", "BlaaBlaa" };

			new SampleData_Index().Execute(documentStore);

			using (var session = documentStore.OpenSession())
			{
				for (int i = 0; i < 600; i++)
				{
					session.Store(new SampleData
					{
						Name = names[i % 5],
						tag = i % 3
					});
				}

				session.SaveChanges();
			}

		}
		public class SampleData
		{
			public string Name { get; set; }
			public int tag { get; set; }
		}

		public class SampleData_Index : AbstractIndexCreationTask<SampleData>
		{
			public SampleData_Index()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  doc.Name,
								  Tag = doc.tag,
								  TotalCount = 1
							  };
				Store(x => x.Name, FieldStorage.Yes);
			}

			public class Result
			{
				public string Name;
			}
		}
	}
}