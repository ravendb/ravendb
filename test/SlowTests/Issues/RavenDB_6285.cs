using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6285 : RavenTestBase
    {
        public RavenDB_6285(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BasicChangesApi()
        {
            using (var store = GetDocumentStore())
            {
                var mre = new ManualResetEventSlim();

                var changes = await store.Changes().EnsureConnectedNow();
                var observable = changes.ForAllDocuments();
                observable.Subscribe(x => mre.Set());
                await observable.EnsureSubscribedNow();

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                Assert.True(mre.Wait(TimeSpan.FromSeconds(45)));
            }
        }
    }
}
