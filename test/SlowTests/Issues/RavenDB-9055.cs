using System;
using System.Threading;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Constants = Raven.Client.Constants;
using Xunit.Abstractions;

namespace SlowTests.Issues
{

    public class RavenDB_9055 : RavenTestBase
    {
        public RavenDB_9055(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AggressivelyCacheWorksWhenTopologyUpdatesIsDisable()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Idan"
                    }, "users/1");
                    session.SaveChanges();
                }

                var mre = new ManualResetEventSlim();
                string changeVector;
                using (documentStore.AggressivelyCache())
                using (var session = documentStore.OpenSession())
                {
                    var forAllDocuments = documentStore.Changes().ForAllDocuments();
                    forAllDocuments.Subscribe(change => mre.Set());
                    Assert.True(forAllDocuments.EnsureSubscribedNow().Wait(TimeSpan.FromMinutes(1)));

                    var user = session.Load<User>("users/1");
                    user.Name = "Shalom";
                    session.SaveChanges();
                    changeVector = session.Advanced.GetMetadataFor(user)?.GetString(Constants.Documents.Metadata.ChangeVector);
                }
                Assert.True(mre.Wait(500));
                Assert.NotNull(changeVector);

                string updateChangeVector = null;
                for (int i = 0; i < 15; i++)
                {
                    using (documentStore.AggressivelyCache())
                    using (var session = documentStore.OpenSession())
                    {
                        var user = session.Load<User>("users/1");
                        updateChangeVector = session.Advanced.GetMetadataFor(user)?
                            .GetString(Constants.Documents.Metadata.ChangeVector);

                        if (updateChangeVector != null && updateChangeVector.Equals(changeVector))
                        {
                            break;
                        }
                        Thread.Sleep(100);
                    }
                }
                Assert.NotNull(updateChangeVector);
                Assert.Equal(changeVector, updateChangeVector);
            }
        }
    }
}
