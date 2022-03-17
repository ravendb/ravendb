using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class DocumentQueryIncludeAndStreamTest : RavenTestBase
    {
        public DocumentQueryIncludeAndStreamTest(ITestOutputHelper output) : base(output)
        {
        }

        public class ProcessStep
        {
            public string Id { get; set; }

            public string StepExecutionsId { get; set; }

            public string DeviceSerial { get; set; }

            public string StepName { get; set; }

            public int Group { get; set; }

            public DateTime Start { get; set; }

            public DateTime Stop { get; set; }
        }

        public class StepExecutions
        {
            public string Id { get; set; }

            public List<StepExecution> Executions { get; set; } = new List<StepExecution>();
        }

        public class StepExecution
        {
            public int Group { get; set; }

            public DateTime ExecutionStopTime { get; set; }
        }

        public class ProcessStepIndex : AbstractIndexCreationTask<ProcessStep>
        {
            public ProcessStepIndex()
            {
                Map = steps => from step in steps
                    let se = LoadDocument<StepExecutions>(step.StepExecutionsId)
                    select new
                    {
                        step.DeviceSerial,
                        step.Group,
                        step.StepName,
                        step.Start,
                        step.Stop,
                        LatestExecution = se.Executions.All(e => e.ExecutionStopTime < step.Stop),
                        LatestExecutionInGroup = se.Executions
                            .Where(e => e.Group == step.Group).All(e => e.ExecutionStopTime < step.Stop)
                    };
            }
        }

        [Fact]
        public void StreamDocumentQueryWithInclude()
        {
            var store = GetDocumentStore();
            Setup(store);
            Indexes.WaitForIndexing(store);
            using (var session = store.OpenSession())
            {
                var query = session.Advanced.DocumentQuery<ProcessStep, ProcessStepIndex>();
                query.WhereEquals("Group", 2);
                query.WhereEquals("LatestExecution", true);
                query.Include(p => p.StepExecutionsId);
                var notSupportedException = Assert.Throws<RavenException>(() =>
                {
                    using (var stream = session.Advanced.Stream(query))
                    {
                        while (stream.MoveNext())
                        {

                        }
                    }
                }).InnerException;
                Assert.Contains("Includes are not supported by this type of query", notSupportedException.Message);
            }
        }

        void Setup(IDocumentStore store)
        {
            var index = new ProcessStepIndex();
            index.Execute(store);
            using (var session = store.OpenSession())
            {
                var currentTime = DateTime.Now;
                session.Store(new StepExecutions
                {
                    Id = "1234/Characterization",
                    Executions = new List<StepExecution>
                    {
                        new StepExecution
                        {
                            ExecutionStopTime = currentTime,
                            Group = 2
                        },
                        new StepExecution
                        {
                            ExecutionStopTime = currentTime.AddHours(-1.0),
                            Group = 1
                        }
                    }
                });
                session.Store(new ProcessStep
                {
                    DeviceSerial = "1234",
                    StepExecutionsId = "1234/Characterization",
                    StepName = "Characterization",
                    Group = 2,
                    Start = currentTime.AddMinutes(-10.0),
                    Stop = currentTime
                });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }
    }
}
