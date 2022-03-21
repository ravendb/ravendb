// -----------------------------------------------------------------------
//  <copyright file="FromUser.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.DistinctFacets
{
    public class FromUser : RavenTestBase
    {
        public FromUser(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldFacetsWork()
        {
            using (var documentStore = GetDocumentStore())
            {
                CreateSampleData(documentStore);
                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var ex = Assert.Throws<InvalidOperationException>(() =>
                    {
                        return session.Advanced.DocumentQuery<SampleData, SampleData_Index>()
                            .Distinct()
                            .SelectFields<SampleData_Index.Result>("Name")
                            .AggregateBy(x => x.ByField("Tag"))
                            .AndAggregateBy(x => x.ByField("TotalCount"))
                            .Execute();
                    });

                    Assert.Equal("Aggregation query can select only facets while it got DistinctToken token", ex.Message);
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
