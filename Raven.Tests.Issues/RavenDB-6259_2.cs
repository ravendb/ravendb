using System.Net.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6259_2 : ReplicationBase
    {
        [Fact]
        public void ShouldNotThrowItemWithTheSameKeyHasAlreadyBeenAdded()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            using (var store3 = CreateStore())
            using (var store4 = CreateStore())
            {
                CreateDoc(store1);
                CreateDoc(store2);
                CreateDoc(store3);
                CreateDoc(store4);

                using (var session = store1.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                TellInstanceToReplicateToAnotherInstance(1, 0);
                Resolve(store1);

                TellInstanceToReplicateToAnotherInstance(2, 0);
                Resolve(store1);

                TellInstanceToReplicateToAnotherInstance(3, 0);
                Resolve(store1);
            }
        }

        private void Resolve(DocumentStore store)
        {
            Assert.True(WaitForConflictDocumentsToAppear(store, "companies/1", store.DefaultDatabase));

            using (var session = store.OpenSession())
            {
                session.Store(new ReplicationConfig
                {
                    DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLocal
                }, Constants.RavenReplicationConfig);

                session.SaveChanges();
            }

            WaitForIndexing(store);

            var url = $"{store.Url.ForDatabase(store.DefaultDatabase)}/replication/forceConflictResolution";
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions));
            var json = request.ReadResponseJson();

            var operation = new Operation((AsyncServerClient)store.AsyncDatabaseCommands, json.Value<long>("OperationId"));
            operation.WaitForCompletion();

            using (var session = store.OpenSession())
            {
                Assert.Null(session.Load<Company>("companies/1"));

                session.Delete(Constants.RavenReplicationConfig);

                session.SaveChanges();
            }
        }

        private static void CreateDoc(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Company { Name = store.Url });

                session.SaveChanges();
            }
        }
    }
}
