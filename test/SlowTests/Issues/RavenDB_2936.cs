// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2936.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2936 : RavenTestBase
    {
        public RavenDB_2936(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldNotRetrieveOperationDetails(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Users/ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Arek"
                    });

                    session.Store(new User
                    {
                        Name = "Oren"
                    });

                    session.Store(new User
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var result = store.Operations.Send(new PatchByQueryOperation(
                        "FROM INDEX 'Users/ByName' UPDATE { this.LastName = 'Smith' }"))
                    .WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(15));

                Assert.Empty(result.Details);

                Indexes.WaitForIndexing(store);

                result = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM INDEX 'Users/ByName'" }))
                    .WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(15));

                Assert.Empty(result.Details);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldRetrieveOperationDetailsWhenTheyWereRequested(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Users/ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Arek"
                    });

                    session.Store(new User
                    {
                        Name = "Oren"
                    });

                    session.Store(new User
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var result = store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = "FROM INDEX 'Users/ByName' UPDATE { this.LastName = 'Smith'}" },
                    new QueryOperationOptions { RetrieveDetails = true }))
                    .WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(15));

                Assert.NotEmpty(result.Details);

                Indexes.WaitForIndexing(store);

                result = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM INDEX 'Users/ByName'" }, new QueryOperationOptions { RetrieveDetails = true }))
                    .WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30));

                Assert.NotEmpty(result.Details);
            }
        }
    }
}
