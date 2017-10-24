using System;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
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

            using (var server = GetNewServer(runInMemory: false))
            {
                using (var documentStore = new DocumentStore
                {
                    Urls = UseFiddler(server.WebUrl),
                    Database = "RavenDB_9055"
                })
                {
                    documentStore.Conventions.DisableTopologyUpdates = true;
                    documentStore.Conventions.UseOptimisticConcurrency = true;
                    documentStore.Initialize();

                    var operation = new CreateDatabaseOperation(new DatabaseRecord(documentStore.Database));
                    documentStore.Admin.Server.Send(operation);

                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Idan"
                        }, "users/1");
                        session.SaveChanges();
                    }

                    string changeVector;
                    using (documentStore.AggressivelyCache())
                    using (var session = documentStore.OpenSession())
                    {
                        documentStore.Changes().ForAllDocuments().EnsureSubscribedNow().Wait();

                        var user = session.Load<User>("users/1");
                        user.Name = "Shalom";
                        session.SaveChanges();
                        changeVector = session.Advanced.GetMetadataFor(user)?.GetString(Constants.Documents.Metadata.ChangeVector);
                    }
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
}
