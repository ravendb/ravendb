// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3390.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.JsConsole;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3390 : RavenTest
    {
        [Fact]
        public void CanGetSettings()
        {
            using (var store = NewDocumentStore())
            {
                var settings = (RavenJObject)new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return { 
                                    RaiseBatchLimit : database.Configuration.AvailableMemoryForRaisingBatchSizeLimit,
                                    ReduceBatchLimit: database.Configuration.MaxNumberOfItemsToReduceInSingleBatch
                                };
                             "
                });

                Assert.NotNull(settings);
                Assert.NotNull(settings["RaiseBatchLimit"]);
                Assert.NotNull(settings["ReduceBatchLimit"]);
            }
        }

        [Fact]
        public void CanModifyConfigurationOnTheFly()
        {
            using (var store = NewDocumentStore())
            {
                var configuration = store.DocumentDatabase.Configuration;

                Assert.False(configuration.DisableDocumentPreFetching);

                new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                database.Configuration.DisableDocumentPreFetching = true;
                                database.Configuration.MaxNumberOfItemsToPreFetch = 13;
                                database.Configuration.BulkImportBatchTimeout = System.TimeSpan.FromMinutes(13);
                             "
                });

                Assert.True(configuration.DisableDocumentPreFetching);
                Assert.Equal(13, configuration.MaxNumberOfItemsToPreFetch);
                Assert.Equal(TimeSpan.FromMinutes(13), configuration.BulkImportBatchTimeout);
            }
        }

        [Fact]
        public void CanPurgeTombstones()
        {
            using (var store = NewDocumentStore())
            {
                var tombstoneRetentionTime = store.DocumentDatabase.Configuration.TombstoneRetentionTime;

                SystemTime.UtcDateTime = () => DateTime.UtcNow.Subtract(tombstoneRetentionTime.Add(tombstoneRetentionTime));

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    accessor.Lists.Set(Constants.RavenPeriodicExportsDocsTombstones, "1", new RavenJObject(), UuidType.Documents);
                    accessor.Lists.Set(Constants.RavenReplicationDocsTombstones, "2", new RavenJObject(), UuidType.Documents);
                });

                SystemTime.UtcDateTime = null;

                new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                database.Maintenance.PurgeOutdatedTombstones();
                             "
                });

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, "1");
                    Assert.Null(tombstone);

                    tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, "2");
                    Assert.Null(tombstone);
                });
            }
        }

        [Fact]
        public void CanRunIdleOperations()
        {
            using (var store = NewDocumentStore())
            {
                var lastIdleTime = store.DocumentDatabase.WorkContext.LastIdleTime;

                new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                database.RunIdleOperations();
                             "
                });

                Assert.NotEqual(lastIdleTime, store.DocumentDatabase.WorkContext.LastIdleTime);
            }
        }

        [Fact]
        public void CanGetStats()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("1", null, new RavenJObject(), new RavenJObject());

                var stats = (RavenJObject)new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.Statistics;
                             "
                });

                Assert.NotNull(stats);
                Assert.Equal(1, stats["CountOfDocuments"]);
            }
        }

        [Fact]
        public void CanPutDocument()
        {
            using (var store = NewDocumentStore())
            {				 						
                new AdminJsConsole(store.DocumentDatabase).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                var doc = Raven.Json.Linq.RavenJObject.Parse('{ ""Name"" : ""Raven"" }');
                                var metadata = Raven.Json.Linq.RavenJObject.Parse('{ ""Raven-Entity-Name"" : ""Docs"" }');
                                database.Documents.Put('doc/1', null, doc, metadata, null, null,Raven.Database.Storage.InvokeSource.Default);
                             "
                });

                var jsonDocument = store.DatabaseCommands.Get("doc/1");

                Assert.NotNull(jsonDocument);
                Assert.Equal("Raven", jsonDocument.DataAsJson["Name"]);
                Assert.Equal("Docs", jsonDocument.Metadata[Constants.RavenEntityName]);
            }
        }
    }
}
