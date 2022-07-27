using System;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17200 : RavenTestBase
    {

        public RavenDB_17200(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_Not_Throw_Nre_When_Compacting_Not_Existing_Index()
        {
            using (var documentStore = GetDocumentStore(new Options
            {
                RunInMemory = false
            }))
            {
                var settings = new CompactSettings
                {
                    DatabaseName = documentStore.Database,
                    Documents = true,
                    Indexes = new[] { "DoesNotExist" }
                };

                var operation = documentStore.Maintenance.Server.Send(new CompactDatabaseOperation(settings));
                operation.WaitForCompletion(TimeSpan.FromMinutes(5));
            }
        }

    }
}
