using System;
using System.Collections.Generic;
using System.Diagnostics;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6369 : RavenTestBase
    {
        [Fact]
        public void ShouldTimeout()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John" });
                    session.SaveChanges();
                }

                var requestExecuter = store.GetRequestExecutor();
                using(var session = store.OpenAsyncSession())
                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new TestQueryCommand((InMemoryDocumentSessionOperations)session, store.Conventions, new IndexQuery { Query = $"FROM INDEX '{new Users_ByName().IndexName}'", WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(100), WaitForNonStaleResults = true });

                    var sw = Stopwatch.StartNew();
                    Assert.Throws<RavenException>(() => requestExecuter.Execute(command, context));
                    sw.Stop();

                    // Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1), sw.Elapsed.ToString()); this can take longer when running tests in parallel but is not needed to assert if the request was cancelled or not
                }
            }
        }

        private class TestQueryCommand : QueryCommand
        {
            public TestQueryCommand(InMemoryDocumentSessionOperations session, DocumentConventions conventions, IndexQuery indexQuery, bool metadataOnly = false, bool indexEntriesOnly = false) : base(session, indexQuery, metadataOnly, indexEntriesOnly)
            {
                Timeout = TimeSpan.FromMilliseconds(100);
            }
        }
    }
}
