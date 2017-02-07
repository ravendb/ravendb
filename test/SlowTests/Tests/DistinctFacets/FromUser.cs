// -----------------------------------------------------------------------
//  <copyright file="FromUser.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.DistinctFacets
{
    public class FromUser : RavenNewTestBase
    {
        [Fact]
        public void ShouldFacetsWork()
        {
            using (var documentStore = GetDocumentStore())
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
                    Assert.Equal(3, result.Results["Tag"].Values.Count);

                    Assert.Equal(5, result.Results["TotalCount"].Values[0].Hits);

                    Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "0").Hits);
                    Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "1").Hits);
                    Assert.Equal(5, result.Results["Tag"].Values.First(x => x.Range == "2").Hits);
                }
            }
        }
        private static void CreateSampleData(IDocumentStore documentStore)
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
                        Tag = i % 3
                    });
                }

                session.SaveChanges();
            }

        }
        private class SampleData
        {
            public string Name { get; set; }
            public int Tag { get; set; }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Name,
                                  Tag = doc.Tag,
                                  TotalCount = 1
                              };
                Store(x => x.Name, FieldStorage.Yes);
                Sort(x => x.Tag, SortOptions.NumericLong);
                Sort("TotalCount", SortOptions.NumericLong);
            }

            public class Result
            {
#pragma warning disable 169,649
                public string Name;
#pragma warning restore 169,649
            }
        }
    }
}
