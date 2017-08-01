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

namespace SlowTests.Issues
{
    public class RavenDB_4091 : RavenTestBase
    {
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

        [Fact(Skip = "RavenDB-6285")]
        public void can_get_in_progress_operation_when_deleting_by_index()
        {
            using (var store = GetDocumentStore())
            {
                new Company_ByName().Execute(store);
                put_1500_companies(store);

                WaitForIndexing(store);

                var operation = store.Operations.Send(new DeleteByIndexOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}'" }));

                var progresses = new List<IOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                Assert.NotEmpty(progresses);

                //Assert.Equal(1500, progresses.Last().ProcessedEntries);
                //Assert.Equal(1500, progresses.Last().TotalEntries);
            }
        }

        [Fact(Skip = "RavenDB-6285")]
        public void can_get_in_progress_operation_when_patching_by_script()
        {
            using (var store = GetDocumentStore())
            {
                put_1500_companies(store);

                WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByIndexOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}'" }, new PatchRequest
                {
                    Script = @"this.Sample = 'Value'"
                }));

                var progresses = new List<IOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                Assert.NotEmpty(progresses);

                //Assert.Equal(1500, progresses.Last().ProcessedEntries);
                //Assert.Equal(1500, progresses.Last().TotalEntries);
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