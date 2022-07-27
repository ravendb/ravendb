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
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2762 : RavenTestBase
    {
        public RavenDB_2762(ITestOutputHelper output) : base(output)
        {
        }

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
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "test",
                    Maps = { "from doc in docs from name in doc.Names select new { Name = name.Length / (name.Length - 1) }" }
                }));

                var errors = Indexes.WaitForIndexingErrors(store, new[]{"test"});
                Assert.NotEmpty(errors);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                var recoveredErrors = Indexes.WaitForIndexingErrors(store, new []{"test"}, TimeSpan.FromSeconds(20));
                Assert.True(recoveredErrors[0].Errors.Length > 0, "Waited for 20s for the index errors to be reloaded after start");
                Assert.NotEmpty(recoveredErrors[0].Errors);

                Assert.Equal(errors[0].Errors.Length, recoveredErrors[0].Errors.Length);

                for (var i = 0; i < errors.Length; i++)
                {
                    Assert.Equal(errors[i].Errors[i].Error, recoveredErrors[i].Errors[i].Error);
                    Assert.Equal(errors[i].Errors[i].Action, recoveredErrors[i].Errors[i].Action);
                    Assert.Equal(errors[i].Errors[i].Document, recoveredErrors[i].Errors[i].Document);
                    Assert.Equal(errors[i].Errors[i].Timestamp, recoveredErrors[i].Errors[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }
    }
}
