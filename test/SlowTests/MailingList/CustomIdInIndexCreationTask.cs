// -----------------------------------------------------------------------
//  <copyright file="CustomIdInIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class CustomIdInIndexCreationTask : RavenTestBase
    {
        public CustomIdInIndexCreationTask(ITestOutputHelper output) : base(output)
        {
        }

        private class Task
        {
            public string id { get; set; }
            public string Name { get; set; }
        }

        private class Task_Index : AbstractIndexCreationTask<Task, Task_Index.TaskIndexData>
        {
            public class TaskIndexData
            {
                public string TaskId { get; set; }
                public string Name { get; set; }
            }

            public Task_Index()
            {
                Map = Tasks => from t in Tasks
                               select new TaskIndexData
                               {
                                   TaskId = t.id,
                                   Name = t.Name,
                               };
            }
        }

        [Fact]
        public void ShouldWork()
        {
            var convention = new DocumentConventions
            {
                FindIdentityProperty = info => info.Name == "id"
            };

            var indexDefinition = new Task_Index
            {
                Conventions = convention
            }.CreateIndexDefinition();

            Assert.Contains("Id(", indexDefinition.Maps.First());
        }


        [Fact]
        public void GenerateCorrectIndex()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.FindIdentityProperty = info => info.Name == "id";
                }
            }))
            {
                new Task_Index().Execute(store);

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("Task/Index"));
                Assert.Contains("Id(", indexDefinition.Maps.First());
            }
        }
    }
}
