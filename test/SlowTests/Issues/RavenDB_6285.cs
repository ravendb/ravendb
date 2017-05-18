using System;
using System.Threading;
using FastTests;
using FastTests.Server.Documents.Notifications;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6285 : RavenTestBase
    {
        [Fact]
        public void BasicChangesApi()
        {
            using (var store = GetDocumentStore())
            {
                var mre = new ManualResetEventSlim();

                var allDocs = store.Changes()
                    .ForAllDocuments()
                    .Subscribe(x => mre.Set());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                Assert.True(mre.Wait(TimeSpan.FromSeconds(5)));
            }
        }
    }
}