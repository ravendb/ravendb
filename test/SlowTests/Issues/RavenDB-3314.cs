using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3314 : RavenTestBase
    {
        public RavenDB_3314(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void set_index_priority(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new SampleIndex
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Normal));

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex");
                    Assert.Equal(IndexPriority.Normal, indexStats.Priority);
                });

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Low));

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex");
                    Assert.Equal(IndexPriority.Low, indexStats.Priority);
                });

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.High));

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex");
                    Assert.Equal(IndexPriority.High, indexStats.Priority);
                });
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void set_index_priority_through_index_definition(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index1 = new SampleIndex1
                {
                    Conventions = new DocumentConventions()
                };
                index1.Execute(store);

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex1");
                    Assert.Equal(IndexPriority.High, indexStats.Priority);
                });

                var index2 = new SampleIndex2
                {
                    Conventions = new DocumentConventions()
                };
                index2.Execute(store);

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex2");
                    Assert.Equal(IndexPriority.Low, indexStats.Priority);
                });

                var index3 = new SampleIndex3
                {
                    Conventions = new DocumentConventions()
                };
                index3.Execute(store);

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex3");
                    Assert.Equal(IndexPriority.Low, indexStats.Priority);
                });

                var index4 = new SampleIndex4
                {
                    Conventions = new DocumentConventions()
                };
                index4.Execute(store);

                tester.AssertAll((_, stats) =>
                {
                    var indexStats = stats.Indexes.First(x => x.Name == "SampleIndex4");
                    Assert.Equal(IndexPriority.Normal, indexStats.Priority);
                });
            }
        }

        private class SampleIndex : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
            }

        }
        private class SampleIndex1 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex1()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexPriority.High;
            }

        }

        private class SampleIndex2 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex2()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexPriority.Low;
            }

        }
        private class SampleIndex3 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex3()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexPriority.Low;
            }

        }
        private class SampleIndex4 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex4()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
            }
        }
        private class Employee
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }
    }
}
