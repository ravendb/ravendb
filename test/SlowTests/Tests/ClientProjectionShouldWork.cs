// -----------------------------------------------------------------------
//  <copyright file="T1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests
{
    public class ClientProjectionShouldWork : RavenTestBase
    {
        public ClientProjectionShouldWork(ITestOutputHelper output) : base(output)
        {
        }

        private class Employee
        {
            public string Id { get; set; }

            public string FirstName { get; set; }
        }

        private class EmployeeCount
        {
            public string FirstName { get; set; }

            public int Count { get; set; }
        }

        private class SimpleMapReduceIndex : AbstractIndexCreationTask<Employee, SimpleMapReduceIndex.Result>
        {
            public class Result
            {
                public string FirstName { get; set; }

                public int Count { get; set; }
            }

            public SimpleMapReduceIndex()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.FirstName,
                                       Count = 1
                                   };

                Reduce = results => from result in results
                                    group result by result.FirstName into g
                                    select new
                                    {
                                        FirstName = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        private void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new SimpleMapReduceIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "F1" });
                    session.Store(new Employee { FirstName = "F1" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results1 = session
                        .Query<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(2, results1[0].Count);
                    Assert.Equal("F1", results1[0].FirstName);

                    results1 = session
                        .Advanced
                        .DocumentQuery<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(2, results1[0].Count);
                    Assert.Equal("F1", results1[0].FirstName);

                    var results2 = session
                        .Query<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>()
                        .Select(x => new EmployeeCount
                        {
                            Count = x.Count,
                            FirstName = x.FirstName
                        })
                        .ToList();

                    Assert.Equal(1, results2.Count);
                    Assert.Equal(2, results2[0].Count);
                    Assert.Equal("F1", results2[0].FirstName);

                    var results3 = session
                        .Advanced
                        .DocumentQuery<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>()
                        .SelectFields<EmployeeCount>()
                        .ToList();

                    Assert.Equal(1, results3.Count);
                    Assert.Equal(2, results3[0].Count);
                    Assert.Equal("F1", results3[0].FirstName);
                }
            }
        }
    }
}
