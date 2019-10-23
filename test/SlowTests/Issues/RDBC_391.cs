using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_391 : RavenTestBase
    {
        public RDBC_391(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseOperationsWithSelectedNodeTagWhenDisableTopologyUpdatesIsTrue()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
            }))
            {
                using (var session = store.OpenSession())
                {
                    var operation = session
                        .Advanced
                        .DocumentStore
                        .Operations.Send(new PatchByQueryOperation("from Ent update { this.Field = 'Abc' }"));

                    operation.WaitForCompletion(TimeSpan.FromSeconds(60));
                }
            }
        }
    }
}
