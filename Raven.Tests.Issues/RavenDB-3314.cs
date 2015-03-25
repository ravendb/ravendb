using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3314 : RavenTestBase
    {
        [Fact]
        public void set_index_priority()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {

                var index = new SampleIndex
                {
                    Conventions = new DocumentConvention()
                };
                index.Execute(store);

                store.DatabaseCommands.SetIndexPriority("SampleIndex", IndexingPriority.Normal);

                var stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexingPriority.Normal, stats.Priority);




                store.DatabaseCommands.SetIndexPriority("SampleIndex", IndexingPriority.Idle);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexingPriority.Idle, stats.Priority);



                store.DatabaseCommands.SetIndexPriority("SampleIndex", IndexingPriority.Disabled);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexingPriority.Disabled, stats.Priority);



                store.DatabaseCommands.SetIndexPriority("SampleIndex", IndexingPriority.Abandoned);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex");

                Assert.Equal(IndexingPriority.Abandoned, stats.Priority); 


            }
        }
        [Fact]
        public void set_index_priority_through_index_definition()
        {

            using (var store = NewRemoteDocumentStore(fiddler: true))
            {

                var index1 = new SampleIndex1
                {
                    Conventions = new DocumentConvention()
                };
                index1.Execute(store);
                var stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex1");
                Assert.Equal(IndexingPriority.Abandoned, stats.Priority);


                var index2 = new SampleIndex2
                {
                    Conventions = new DocumentConvention()
                };
                index2.Execute(store);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex2");

                Assert.Equal(IndexingPriority.Idle, stats.Priority);



                var index3 = new SampleIndex3
                {
                    Conventions = new DocumentConvention()
                };
                index3.Execute(store);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex3");

                Assert.Equal(IndexingPriority.Disabled, stats.Priority);



                var index4 = new SampleIndex4
                {
                    Conventions = new DocumentConvention()
                };
                index4.Execute(store);

                stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "SampleIndex4");

                Assert.Equal(IndexingPriority.Normal, stats.Priority);

            }
        }

        public class SampleIndex : AbstractIndexCreationTask<Employee>
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
        public class SampleIndex1 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex1()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexingPriority.Abandoned;
            }

        }

        public class SampleIndex2 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex2()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexingPriority.Idle;
            }

        }
        public class SampleIndex3 : AbstractIndexCreationTask<Employee>
        {
            public SampleIndex3()
            {
                Map = employees => from employee in employees
                                   select new
                                   {
                                       employee.Name,
                                       employee.Address
                                   };
                Priority = IndexingPriority.Disabled;
            }

        }
        public class SampleIndex4 : AbstractIndexCreationTask<Employee>
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
        public class Employee
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }
    }
}
