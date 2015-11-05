// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2710.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Tests.Common.Dto;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2710 : RavenTestBase
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup;Replication";
        }

        [Fact]
        public void ShouldPurgeTombstones()
        {
            using (var store = NewDocumentStore())
            {
                SystemTime.UtcDateTime = () => DateTime.UtcNow.Subtract(store.Configuration.TombstoneRetentionTime);

                // create document
                string user1;
                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "arek"
                    };
                    session.Store(user);

                    user1 = user.Id;
                    session.SaveChanges();
                }

                //now delete it to create tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete(user1);
                    session.SaveChanges();
                }

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, user1);
                    Assert.NotNull(tombstone);
                    tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, user1);
                    Assert.NotNull(tombstone);
                });

                SystemTime.UtcDateTime = () => DateTime.UtcNow;

                string user2;
                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "ayende"
                    };
                    session.Store(user);

                    user2 = user.Id;

                    session.SaveChanges();
                }

                //now delete it to create tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete(user2);
                    session.SaveChanges();
                }

                store.DocumentDatabase.Maintenance.PurgeOutdatedTombstones();

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, user1);
                    Assert.Null(tombstone);

                    tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, user1);
                    Assert.Null(tombstone);

                    tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, user2);
                    Assert.NotNull(tombstone);

                    tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, user2);
                    Assert.NotNull(tombstone);
                });

            }
        }
    }
}
