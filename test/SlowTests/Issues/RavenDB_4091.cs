// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4091.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4091 : RavenTestBase
    {
        public RavenDB_4091(ITestOutputHelper output) : base(output)
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

        [Fact]
        public void can_get_in_progress_operation_when_deleting_by_index()
        {
            using (var store = GetDocumentStore())
            {
                new Company_ByName().Execute(store);
                put_1500_companies(store);

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}'" }, new QueryOperationOptions()
                {
                    // let us slow down the operation a bit to make sure we'll get "in-progress" notification before "completed"
                    MaxOpsPerSecond = 500
                }));

                var progresses = new List<IOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion(TimeSpan.FromSeconds(300));

                Assert.NotEmpty(progresses);

                var result = progresses.Last().ToJson();
                Assert.Equal(1500L, result["Processed"]);
                Assert.Equal(1500L, result["Total"]);
            }
        }

        [Fact]
        public void can_get_in_progress_operation_when_patching_by_script()
        {
            using (var store = GetDocumentStore())
            {
                new Company_ByName().Execute(store);
                put_1500_companies(store);

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}' UPDATE {{this.Sample = 'Value'}}" }));

                Assert.Equal(OperationStatusFetchMode.ChangesApi, operation.StatusFetchMode);

                var progresses = new List<IOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                Assert.NotEmpty(progresses);

                var result = progresses.Last().ToJson();
                Assert.Equal(1500L, result["Processed"]);
                Assert.Equal(1500L, result["Total"]);
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
