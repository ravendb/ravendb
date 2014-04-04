// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1749.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Net;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1749 : ReplicationBase
    {
        [Fact]
        public void PassingInvalidDocEtagDoesNotIgnoreAttachmentEtagWhenPurgingTombstones()
        {
            var store1 = CreateStore(databaseName: Constants.SystemDatabase);

            store1.DatabaseCommands.PutAttachment("attachment/1", null, new MemoryStream(), new RavenJObject());
            store1.DatabaseCommands.DeleteAttachment("attachment/1", null);

            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => Assert.NotEmpty(accessor.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, Etag.Empty, null, 10)));

            Etag lastAttachmentEtag = Etag.Empty.Setup(UuidType.Attachments, 1).IncrementBy(3);

            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                              servers[0].SystemDatabase.ServerUrl +
                                                                              string.Format("admin/replication/purge-tombstones?docEtag={0}&attachmentEtag={1}", null, lastAttachmentEtag),
                                                                              "POST",
                                                                              new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                              store1.Conventions);
            store1.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();


            servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => Assert.Empty(accessor.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, Etag.Empty, null, 10)));
        }
    }
}