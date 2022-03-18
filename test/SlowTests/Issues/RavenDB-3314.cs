using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
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

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Normal));

                AssertIndex(store, options, "SampleIndex", stats => Assert.Equal(IndexPriority.Normal, stats.Priority));

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Low));

                AssertIndex(store, options, "SampleIndex", stats => Assert.Equal(IndexPriority.Low, stats.Priority));

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.High));

                AssertIndex(store, options, "SampleIndex", stats => Assert.Equal(IndexPriority.High, stats.Priority));
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

                AssertIndex(store, options, "SampleIndex1", stats => Assert.Equal(IndexPriority.High, stats.Priority));

                var index2 = new SampleIndex2
                {
                    Conventions = new DocumentConventions()
                };
                index2.Execute(store);

                AssertIndex(store, options, "SampleIndex2", stats => Assert.Equal(IndexPriority.Low, stats.Priority));

                var index3 = new SampleIndex3
                {
                    Conventions = new DocumentConventions()
                };
                index3.Execute(store);

                AssertIndex(store, options, "SampleIndex3", stats => Assert.Equal(IndexPriority.Low, stats.Priority));

                var index4 = new SampleIndex4
                {
                    Conventions = new DocumentConventions()
                };
                index4.Execute(store);

                AssertIndex(store, options, "SampleIndex4", stats => Assert.Equal(IndexPriority.Normal, stats.Priority));
            }
        }

        private static void AssertIndex(IDocumentStore store, Options options, string indexName, Action<IndexInformation> assert)
        {
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

                for (var i = 0; i < databaseRecord.Shards.Length; i++)
                {
                    assert(store.Maintenance.ForShard(i).Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == indexName));
                }

                return;
            }

            assert(store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == indexName));
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
