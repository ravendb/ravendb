using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.MultiTenancy
{
    public class NoCaseSensitive : RavenTestBase
    {
        public NoCaseSensitive(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanAccessDbUsingDifferentNames()
        {
            DoNotReuseServer();            
            using (var documentStore = GetDocumentStore())
            {
                var doc = new DatabaseRecord("repro");
                documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                using (var session = documentStore.OpenSession("repro"))
                {
                    session.Store(new Foo
                    {
                        Bar = "test"
                    });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession("Repro"))
                {
                    Assert.NotNull(session.Load<Foo>("foos/1-A"));
                }
            }
            
        }

        private class Foo
        {
            public string Bar { get; set; }
        }
    }
}
