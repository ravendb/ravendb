using FastTests;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Xunit;

namespace SlowTests.Bugs.MultiTenancy
{
    public class NoCaseSensitive : RavenNewTestBase
    {
        [Fact]
        public void CanAccessDbUsingDifferentNames()
        {
            DoNotReuseServer();            
            using (var documentStore = GetDocumentStore())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("repro");
                documentStore.Admin.Send(new CreateDatabaseOperation(doc));

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
                    Assert.NotNull(session.Load<Foo>("foos/1"));
                }
            }
            
        }

        private class Foo
        {
            public string Bar { get; set; }
        }
    }
}
