// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2762 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2762 : RavenTestBase
    {
        [Fact]
        public void IndexingErrorsShouldSurviveDbRestart()
        {
            var dataDir = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = dataDir
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Names = new[] { "a", "b" } });
                    session.SaveChanges();
                }
                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "test",
                    Maps = { "from doc in docs from name in doc.Names select new { Name = name.Length / (name.Length - 1) }" }
                }));

                WaitForIndexing(store);

                IndexingError[] errors = null;
                var result = SpinWait.SpinUntil(() =>
                {
                    errors = store.Admin.Send(new GetIndexErrorsOperation(new[] { "test" }))[0].Errors;
                    return errors?.Length > 0;
                }, TimeSpan.FromSeconds(20));

                Assert.True(result, "Waited for 20 seconds for errors to be persisted but it did not happen.");
                Assert.NotEmpty(errors);

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);

                var recoveredErrors = store.Admin.Send(new GetIndexErrorsOperation(new[] { "test" }))[0].Errors;

                Assert.NotEmpty(recoveredErrors);

                Assert.Equal(errors.Length, recoveredErrors.Length);

                for (var i = 0; i < errors.Length; i++)
                {
                    Assert.Equal(errors[i].Error, recoveredErrors[i].Error);
                    Assert.Equal(errors[i].Action, recoveredErrors[i].Action);
                    Assert.Equal(errors[i].Document, recoveredErrors[i].Document);
                    Assert.Equal(errors[i].Timestamp, recoveredErrors[i].Timestamp);
                }
            }
        }
    }
}
