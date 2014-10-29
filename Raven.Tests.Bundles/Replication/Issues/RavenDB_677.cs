using System.Linq;
using System.Net;

using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bundles.Replication.Issues
{
    using Raven.Abstractions.Connection;

    public class RavenDB_677 : ReplicationBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void CanDeleteTombstones(string requestedStorage)
        {
            var store1 = (DocumentStore)CreateStore(databaseName: Constants.SystemDatabase, requestedStorageType: requestedStorage);
            var x = store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("ayende", null);
            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
            {
                Assert.NotEmpty(accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10));
            });

            Etag last = Etag.Empty.Setup(UuidType.Documents, 1).IncrementBy(3);
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                              servers[0].SystemDatabase.ServerUrl +
                                                                              "admin/replication/purge-tombstones?docEtag=" + last,
                                                                              "POST",
                                                                              new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                              store1.Conventions);
            store1.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();


            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
            {
                Assert.Empty(accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).ToArray());
            });
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanDeleteTombstonesButNotAfterTheSpecifiedEtag(string requestedStorage)
        {
            var store1 = (DocumentStore)CreateStore(databaseName: Constants.SystemDatabase, requestedStorageType: requestedStorage);
            store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("ayende", null);
            store1.DatabaseCommands.Put("rahien", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("rahien", null);
            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
            {
                var count = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).Count();
                Assert.Equal(2, count);
            });

            Etag last = Etag.Empty.Setup(UuidType.Documents, 1).IncrementBy(3);

            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                              servers[0].SystemDatabase.ServerUrl +
                                                                              "admin/replication/purge-tombstones?docEtag=" + last,
                                                                              "POST",
                                                                              new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                              store1.Conventions);
            store1.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();


            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
            {
                Assert.Equal(1, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).Count());
            });
        }
    }
}
