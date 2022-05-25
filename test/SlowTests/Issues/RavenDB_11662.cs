using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11662 : RavenTestBase
    {
        public RavenDB_11662(ITestOutputHelper output) : base(output)
        {
        }

        private class Company_ByName : AbstractIndexCreationTask<Company>
        {
            public Company_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanUsePollingForFetchingOperationStatus(Options options)
        {
            options.ModifyDocumentStore = documentStore => documentStore.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.Polling;
            using (var store = GetDocumentStore(options))
            {
                new Company_ByName().Execute(store);
                put_1500_companies(store);

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}'" }, new QueryOperationOptions
                {
                    // let us slow down the operation a bit to make sure we'll get "in-progress" notification before "completed"
                    MaxOpsPerSecond = 500
                }));

                Assert.Equal(OperationStatusFetchMode.Polling, operation.StatusFetchMode);

                var progresses = new List<IOperationProgress>();

                operation.OnProgressChanged += (_, progress) =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                Assert.NotEmpty(progresses);
            }
        }

        private static void put_1500_companies(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 1500; i++)
                {
                    session.Store(new Company { Name = $"Company {i}" });
                }

                session.SaveChanges();
            }
        }
    }
}
