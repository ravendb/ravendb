using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3314 : RavenTestBase
    {
        public RavenDB_3314(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void set_index_priority()
        {
            using (var store = GetDocumentStore())
            {
                var index = new SampleIndex
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Normal));

                var stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexPriority.Normal, stats.Priority);

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.Low));

                stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexPriority.Low, stats.Priority);

                store.Maintenance.Send(new SetIndexesPriorityOperation("SampleIndex", IndexPriority.High));

                stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexPriority.High, stats.Priority);
            }
        }
        [Fact]
        public void set_index_priority_through_index_definition()
        {
            using (var store = GetDocumentStore())
            {
                var index1 = new SampleIndex1
                {
                    Conventions = new DocumentConventions()
                };
                index1.Execute(store);

                var stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex1");
                Assert.Equal(IndexPriority.High, stats.Priority);

                var index2 = new SampleIndex2
                {
                    Conventions = new DocumentConventions()
                };
                index2.Execute(store);

                stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex2");
                Assert.Equal(IndexPriority.Low, stats.Priority);

                var index3 = new SampleIndex3
                {
                    Conventions = new DocumentConventions()
                };
                index3.Execute(store);

                stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex3");
                Assert.Equal(IndexPriority.Low, stats.Priority);

                var index4 = new SampleIndex4
                {
                    Conventions = new DocumentConventions()
                };
                index4.Execute(store);

                stats = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "SampleIndex4");
                Assert.Equal(IndexPriority.Normal, stats.Priority);
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
