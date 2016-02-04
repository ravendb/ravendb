// -----------------------------------------------------------------------
//  <copyright file="CustomIdInIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class CustomIdInIndexCreationTask : RavenTest
    {

        public class Task
        {
            public string id { get; set; }
            public string Name { get; set; }
        }

        public class Task_Index : AbstractIndexCreationTask<Task, Task_Index.TaskIndexData>
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
            var convention = new DocumentConvention
            {
                FindIdentityProperty = info => info.Name == "id"
            };

            var indexDefinition = new Task_Index
            {
                Conventions = convention
            }.CreateIndexDefinition();

            Assert.Contains("__document_id", indexDefinition.Map);
        }


        [Fact]
        public void GenerateCorrectIndex()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.Conventions.FindIdentityProperty = info => info.Name == "id";
                new Task_Index().Execute(store);

                var indexDefinition = store.DatabaseCommands.GetIndex("Task/Index");
                Assert.Contains("__document_id", indexDefinition.Map);
        
            }
        }
    }
}
