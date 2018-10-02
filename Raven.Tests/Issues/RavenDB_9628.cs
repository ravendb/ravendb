// -----------------------------------------------------------------------
//  <copyright file="RavenDB_9628.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
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

        [Fact]
        public void CanPurgeTombstonesOlderThan()
        {
            var store1 = (DocumentStore)CreateStore();
            store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("ayende", null);

            var now = SystemTime.UtcNow;

            SystemTime.UtcDateTime = () => now.AddDays(1);

            store1.DatabaseCommands.Put("rahien", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("rahien", null);
            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                accessor.Lists.RemoveAllOlderThan(Constants.RavenReplicationDocsTombstones, now);
            });

            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                Assert.Equal(1, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).Count());

                var item = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).First();

                Assert.Equal("rahien", item.Key);
            });
        }

        [Fact]
        public void CanPurgeTombstonesOlderThanUsingMethodCalledByTask()
        {
            var store1 = (DocumentStore)CreateStore();
            store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("ayende", null);

            var now = SystemTime.UtcNow;

            SystemTime.UtcDateTime = () => now.Add(servers[0].Database.Configuration.TombstoneRetentionTime);

            store1.DatabaseCommands.Put("rahien", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Delete("rahien", null);
            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                accessor.Lists.RemoveAllOlderThan(Constants.RavenReplicationDocsTombstones, now);
            });

            servers[0].Database.PurgeOutdatedTombstones();

            servers[0].Database.TransactionalStorage.Batch(accessor =>
            {
                Assert.Equal(1, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).Count());

                var item = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Etag.Empty, null, 10).First();

                Assert.Equal("rahien", item.Key);
            });
        }

        public override void Dispose()
        {
            base.Dispose();

            SystemTime.UtcDateTime = null;

        }
    }
}