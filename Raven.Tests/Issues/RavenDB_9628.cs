// -----------------------------------------------------------------------
//  <copyright file="RavenDB_9628.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_9628 : ReplicationBase
    {
        protected override void ConfigureServer(Database.Config.RavenConfiguration serverConfiguration)
        {
            serverConfiguration.DefaultStorageTypeName = "esent";
            serverConfiguration.RunInMemory = false;
        }

        [Fact]
        public void CanPurgeTombstones()
        {
            var store1 = (DocumentStore)CreateStore();
            store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("ayende", null);
            store1.DatabaseCommands.Put("rahien", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("rahien", null);
            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                accessor.Lists.RemoveAllOlderThan(Constants.RavenReplicationDocsTombstones, DateTime.MaxValue);
            });

            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                Assert.Equal(0, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).Count());
            });
        }
    }
}