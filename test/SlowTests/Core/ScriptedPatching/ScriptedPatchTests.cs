using System;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Core.ScriptedPatching
{
    public class ScriptedPatchTests : RavenTestBase
    {
        [Fact]
        public void PatchingWithParametersShouldWork()
        {
            var store = this.GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Company
                {
                    Name = "The Wall"
                }, "companies/1");

                session.SaveChanges();
            }

            var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { this.Name = args.name }",
                QueryParameters = new Parameters()
                {
                    {"name", "Jon"}
                }
            }));

            operation.WaitForCompletion(TimeSpan.FromSeconds(15));

            using (var session = store.OpenSession())
            {
                var c = session.Load<Company>("companies/1");

                Assert.Equal("Jon", c.Name);
            }
        }
    }
}
