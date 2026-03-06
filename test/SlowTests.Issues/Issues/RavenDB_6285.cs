using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6285 : RavenTestBase
    {
        public RavenDB_6285(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ChangesApi)]
        public async Task BasicChangesApi()
        {
            using (var store = GetDocumentStore())
            {
                var amre = new AsyncManualResetEvent();

                var changes = await store.Changes().EnsureConnectedNow();
                var observable = changes.ForAllDocuments();
                observable.Subscribe(x => amre.Set());
                await observable.EnsureSubscribedNow();

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                Assert.True(await amre.WaitAsync(TimeSpan.FromSeconds(45)));
            }
        }
    }
}
