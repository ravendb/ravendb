using System;
using System.Collections.Generic;
using System.Diagnostics;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
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

                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John" });
                    session.SaveChanges();
                }

                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new TestQueryCommand(store.Conventions, context, new IndexQuery { Query = $"FROM INDEX '{new Users_ByName().IndexName}'", WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(100), WaitForNonStaleResults = true });

                    var sw = Stopwatch.StartNew();
                    Assert.Throws<TimeoutException>(() => requestExecuter.Execute(command, context));
                    sw.Stop();

                    // Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1), sw.Elapsed.ToString()); this can take longer when running tests in parallel but is not needed to assert if the request was cancelled or not
                }
            }
        }

        private class TestQueryCommand : QueryCommand
        {
            public TestQueryCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery indexQuery, HashSet<string> includes = null, bool metadataOnly = false, bool indexEntriesOnly = false) : base(conventions, context, indexQuery, includes, metadataOnly, indexEntriesOnly)
            {
                Timeout = TimeSpan.FromMilliseconds(100);
            }
        }
    }
}