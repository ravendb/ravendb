using System;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Threading;
using Lucene.Net.Util;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Constants = Raven.Client.Constants;

namespace FastTests.Issues
{

    public class RavenDB_9055 : RavenTestBase
    {
        [Fact]
        public void AggressivelyCacheWorksWhenTopologyUpdatesIsDisable()
        {
            using (var documentStore = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.UseOptimisticConcurrency = true;
                    store.Conventions.DisableTopologyUpdates = true;
                }
            }))
            {
                using (documentStore.AggressivelyCache())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Idan"
                        }, "users/1");
                        session.SaveChanges();
                    }
                }

                string changeVector;
                using (documentStore.AggressivelyCache())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        var user = session.Load<User>("users/1");
                        user.Name = "Shalom";
                        session.SaveChanges();
                        changeVector = session.Advanced.GetMetadataFor(user)?.GetString(Constants.Documents.Metadata.ChangeVector);
                    }
                    Assert.NotNull(changeVector);
                }
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
                if (changeVector != updateChangeVector)
                {
                    Console.WriteLine(1);
                }
                Assert.Equal(changeVector, updateChangeVector);
            }
        }
    }
}
